// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package deviceuse

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"regexp"
	"time"
	"unicode"
	"unicode/utf8"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/util/validation"

	pb "local-csi-driver/internal/deviceuse/usev1"
)

type Scope struct {
	Driver, Namespace, NamespaceUID, Node, NodeUID string
}

const (
	useHistoryLimit = 768 * 1024
	journalLimit    = 960 * 1024
)

func (s Scope) Valid() bool {
	return s.Driver != "" && len(s.Driver) <= 63 && len(validation.IsDNS1123Subdomain(s.Driver)) == 0 &&
		s.Namespace != "" && len(validation.IsDNS1123Label(s.Namespace)) == 0 && boundedText(s.NamespaceUID, 128) &&
		s.Node != "" && len(validation.IsDNS1123Subdomain(s.Node)) == 0 && boundedText(s.NodeUID, 128)
}

type Resource struct {
	Key, Target, Group string
}

type Record struct {
	Identity *pb.UseIdentity
	State    pb.UseState
	Resource Resource
}

type Native struct {
	Token, Method, Target, Group string
}

type Snapshot struct {
	Version int
	Scope   Scope
	Uses    map[string]Record
	Busy    *Native
	Last    *Native
}

func Initial(scope Scope) (*Snapshot, error) {
	if !scope.Valid() {
		return nil, status.Error(codes.InvalidArgument, "independent node and namespace scope required")
	}
	return &Snapshot{Version: 1, Scope: scope, Uses: map[string]Record{}}, nil
}

func (s *Snapshot) Validate(scope Scope) error {
	if !scope.Valid() || s.Version != 1 || s.Scope != scope || s.Uses == nil || len(s.Uses) > 4096 {
		return status.Error(codes.Unavailable, "incompatible durable use journal")
	}
	active := map[string]bool{}
	for key, r := range s.Uses {
		if validateIdentity(scope, r.Identity) != nil || key != r.Identity.UseId ||
			r.State != pb.UseState_USE_STATE_ACTIVE && r.State != pb.UseState_USE_STATE_RELEASED ||
			r.State == pb.UseState_USE_STATE_ACTIVE && (r.Resource.Key == "" || r.Resource.Target == "") {
			return status.Error(codes.Unavailable, "invalid durable use record")
		}
		if r.State == pb.UseState_USE_STATE_ACTIVE {
			if active[r.Resource.Key] {
				return status.Error(codes.Unavailable, "duplicate active native resource")
			}
			active[r.Resource.Key] = true
		}
	}
	for _, n := range []*Native{s.Busy, s.Last} {
		if n != nil && (n.Token == "" || n.Method == "") {
			return status.Error(codes.Unavailable, "invalid native admission")
		}
	}
	return nil
}

// Transact must serialize admission with use changes, persist before returning,
// reject incompatible/missing journals, and never retry an unknown write outcome.
type Store interface {
	Transact(context.Context, func(*Snapshot) (bool, error)) error
}

type Resolver interface {
	CheckScope(context.Context, *pb.UseIdentity) error
	Resolve(context.Context, *pb.UseIdentity) (Resource, error)
}

type Domain struct {
	pb.UnimplementedDeviceUseServer
	Scope    Scope
	Store    Store
	Resolver Resolver
	Match    func(string, string) bool
}

var compositeHandle = regexp.MustCompile(`^compositedisk-[a-z2-7]{52}\.[A-Za-z0-9][A-Za-z0-9_.-]{0,60}$`)

func boundedText(s string, max int) bool {
	if s == "" || len(s) > max || !utf8.ValidString(s) {
		return false
	}
	for _, c := range s {
		if unicode.IsControl(c) {
			return false
		}
	}
	return true
}

func validateIdentity(scope Scope, id *pb.UseIdentity) error {
	if id == nil || id.Source == nil || len(id.ProtoReflect().GetUnknown()) != 0 ||
		len(id.Source.ProtoReflect().GetUnknown()) != 0 || !boundedText(id.UseId, 128) ||
		!compositeHandle.MatchString(id.CompositeId) || id.NodeId != scope.Node ||
		id.Source.Driver != scope.Driver || !boundedText(id.Source.VolumeHandle, 2048) ||
		!boundedText(id.Source.PvUid, 128) || id.Source.PvName == "" ||
		len(validation.IsDNS1123Subdomain(id.Source.PvName)) != 0 {
		return status.Error(codes.InvalidArgument, "exact scoped use identity required")
	}
	for _, c := range id.UseId {
		if c < 0x20 || c > 0x7e {
			return status.Error(codes.InvalidArgument, "use ID must be printable ASCII")
		}
	}
	return nil
}

func reply(r Record) *pb.Use {
	return &pb.Use{Identity: proto.CloneOf(r.Identity), State: r.State}
}

func (d *Domain) apply(ctx context.Context, id *pb.UseIdentity, operation string) (*pb.Use, error) {
	if err := validateIdentity(d.Scope, id); err != nil {
		return nil, err
	}
	var result *pb.Use
	err := d.Store.Transact(ctx, func(s *Snapshot) (bool, error) {
		if r, ok := s.Uses[id.UseId]; ok {
			if !proto.Equal(r.Identity, id) {
				return false, status.Error(codes.AlreadyExists, "use ID is permanently bound to another identity")
			}
			if operation == "acquire" && r.State == pb.UseState_USE_STATE_RELEASED {
				return false, status.Error(codes.FailedPrecondition, "released use cannot be acquired again")
			}
			if operation == "acquire" {
				if s.Busy != nil {
					return false, status.Error(codes.Unavailable, "native operation outcome is still retained")
				}
				current, err := d.Resolver.Resolve(ctx, id)
				if err != nil {
					return false, err
				}
				if current != r.Resource {
					return false, status.Error(codes.FailedPrecondition, "active source incarnation changed")
				}
			}
			changed := operation == "release" && r.State == pb.UseState_USE_STATE_ACTIVE
			if changed {
				r.State = pb.UseState_USE_STATE_RELEASED
				s.Uses[id.UseId] = r
			}
			result = reply(r)
			return changed, nil
		}
		if operation == "get" {
			return false, status.Error(codes.NotFound, "use ID has no durable record")
		}
		if len(s.Uses) >= 4096 {
			return false, status.Error(codes.ResourceExhausted, "use journal full; tombstones must not be discarded")
		}
		r := Record{Identity: proto.CloneOf(id), State: pb.UseState_USE_STATE_RELEASED}
		if operation == "acquire" {
			if s.Busy != nil {
				return false, status.Error(codes.Unavailable, "native operation outcome is still retained")
			}
			resource, err := d.Resolver.Resolve(ctx, id)
			if err != nil {
				return false, err
			}
			if resource.Key == "" || resource.Target == "" {
				return false, status.Error(codes.Unavailable, "native source identity is unconfirmed")
			}
			for _, existing := range s.Uses {
				if existing.State == pb.UseState_USE_STATE_ACTIVE && existing.Resource.Key == resource.Key {
					return false, status.Error(codes.AlreadyExists, "actual source resource already has an active use")
				}
			}
			r.State, r.Resource = pb.UseState_USE_STATE_ACTIVE, resource
		} else if err := d.Resolver.CheckScope(ctx, id); err != nil {
			return false, err
		}
		s.Uses[id.UseId] = r
		history, marshalErr := json.Marshal(struct {
			Scope Scope
			Uses  map[string]Record
		}{s.Scope, s.Uses})
		if marshalErr != nil || len(history) > useHistoryLimit {
			return false, status.Error(codes.ResourceExhausted, "use history full; native cleanup headroom is reserved")
		}
		result = reply(r)
		return true, nil
	})
	return result, err
}

func (d *Domain) AcquireUse(ctx context.Context, q *pb.AcquireUseRequest) (*pb.Use, error) {
	if q == nil || len(q.ProtoReflect().GetUnknown()) != 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid acquire request")
	}
	return d.apply(ctx, q.Identity, "acquire")
}
func (d *Domain) GetUse(ctx context.Context, q *pb.GetUseRequest) (*pb.Use, error) {
	if q == nil || len(q.ProtoReflect().GetUnknown()) != 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid get request")
	}
	return d.apply(ctx, q.Identity, "get")
}
func (d *Domain) ReleaseUse(ctx context.Context, q *pb.ReleaseUseRequest) (*pb.Use, error) {
	if q == nil || len(q.ProtoReflect().GetUnknown()) != 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid release request")
	}
	return d.apply(ctx, q.Identity, "release")
}

type tokenKey struct{}
type token struct {
	domain *Domain
	native Native
}

func (d *Domain) blocked(s *Snapshot, target, group string) bool {
	match := d.Match
	if match == nil {
		match = func(a, b string) bool { return a == b }
	}
	for _, r := range s.Uses {
		if r.State != pb.UseState_USE_STATE_ACTIVE {
			continue
		}
		if target == "" && group == "" || target != "" && match(target, r.Resource.Target) ||
			group != "" && group == r.Resource.Group {
			return true
		}
	}
	return false
}

// BeginNative covers the complete destructive operation, not a check followed
// by an unguarded effect. Nested helpers reuse the token but recheck targets.
func (d *Domain) BeginNative(ctx context.Context, method, target, group string) (context.Context, func(bool) error, error) {
	return d.begin(ctx, method, target, group, "", false, false)
}

func (d *Domain) BeginPreparation(ctx context.Context, method, target, node string, block bool) (context.Context, func(bool) error, error) {
	return d.begin(ctx, method, target, "", node, block, true)
}

func (d *Domain) BeginPreparationInGroup(ctx context.Context, method, target, group, node string, block bool) (context.Context, func(bool) error, error) {
	return d.begin(ctx, method, target, group, node, block, true)
}

func (d *Domain) begin(ctx context.Context, method, target, group, node string, block, prepare bool) (context.Context, func(bool) error, error) {
	if d == nil {
		return ctx, func(bool) error { return nil }, nil
	}
	if !boundedText(method, 128) || len(target) > 4096 || len(group) > 4096 {
		return ctx, nil, status.Error(codes.InvalidArgument, "native admission identity exceeds journal bounds")
	}
	prior, nested := ctx.Value(tokenKey{}).(token)
	nested = nested && prior.domain == d
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return ctx, nil, err
	}
	n := Native{Token: hex.EncodeToString(nonce[:]), Method: method, Target: target, Group: group}
	err := d.Store.Transact(ctx, func(s *Snapshot) (bool, error) {
		if nested {
			if s.Busy == nil || *s.Busy != prior.native {
				return false, status.Error(codes.Aborted, "native admission changed")
			}
		} else if s.Busy != nil {
			return false, status.Error(codes.Unavailable, "native operation outcome retained")
		}
		if prepare {
			match := d.Match
			if match == nil {
				match = func(a, b string) bool { return a == b }
			}
			for _, r := range s.Uses {
				if group != "" && r.State == pb.UseState_USE_STATE_ACTIVE && r.Resource.Group == group && !match(target, r.Resource.Target) {
					return false, status.Error(codes.FailedPrecondition, "ambiguous native resource alias")
				}
			}
			if err := d.prepare(ctx, s, target, node, block); err != nil {
				return false, err
			}
		} else if d.blocked(s, target, group) {
			return false, status.Error(codes.FailedPrecondition, "source has an active composite use")
		}
		if nested {
			return false, nil
		}
		s.Busy = &n
		return true, nil
	})
	if err != nil {
		return ctx, nil, err
	}
	if nested {
		return ctx, func(bool) error { return nil }, nil
	}
	ctx = context.WithValue(ctx, tokenKey{}, token{d, n})
	return ctx, func(success bool) error {
		if !success {
			return nil // Ambiguous effects remain admitted across restart.
		}
		// Record a known native success even if its caller lost the reply.
		completeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		err := d.Store.Transact(completeCtx, func(s *Snapshot) (bool, error) {
			if s.Last != nil && *s.Last == n {
				return false, nil
			}
			if s.Busy == nil || *s.Busy != n {
				return false, status.Error(codes.Aborted, "native completion does not own admission")
			}
			s.Last, s.Busy = &n, nil
			return true, nil
		})
		if err == nil {
			return nil
		}
		reconciled := false
		readErr := d.Store.Transact(completeCtx, func(s *Snapshot) (bool, error) {
			reconciled = s.Last != nil && *s.Last == n
			return false, nil
		})
		if readErr == nil && reconciled {
			return nil
		}
		return errors.Join(err, readErr)
	}, nil
}

// Preparation is not a new grant. A held source must still be the exact
// existing native object; missing media must not fall through to provisioning.
func (d *Domain) CheckPrepare(ctx context.Context, target string) error {
	if d == nil {
		return nil
	}
	return d.Prepare(ctx, target, d.Scope.Node, true)
}

func (d *Domain) Prepare(ctx context.Context, target, node string, block bool) error {
	if d == nil {
		return nil
	}
	return d.Store.Transact(ctx, func(s *Snapshot) (bool, error) {
		token, nested := ctx.Value(tokenKey{}).(token)
		if s.Busy != nil && (!nested || token.domain != d || *s.Busy != token.native) {
			return false, status.Error(codes.Unavailable, "native operation outcome retained")
		}
		return false, d.prepare(ctx, s, target, node, block)
	})
}

func (d *Domain) prepare(ctx context.Context, s *Snapshot, target, node string, block bool) error {
	match := d.Match
	if match == nil {
		match = func(a, b string) bool { return a == b }
	}
	for _, r := range s.Uses {
		if r.State != pb.UseState_USE_STATE_ACTIVE || !match(target, r.Resource.Target) {
			continue
		}
		if node != r.Identity.NodeId || !block {
			return status.Error(codes.FailedPrecondition, "held source requires block publication on its owning node")
		}
		current, err := d.Resolver.Resolve(ctx, r.Identity)
		if err != nil {
			return err
		}
		if current != r.Resource {
			return status.Error(codes.FailedPrecondition, "held native source incarnation changed")
		}
	}
	return nil
}

func (d *Domain) CheckUnheld(ctx context.Context, target, group string) error {
	t, ok := ctx.Value(tokenKey{}).(token)
	if !ok || t.domain != d {
		return status.Error(codes.Unavailable, "complete native operation admission required")
	}
	return d.Store.Transact(ctx, func(s *Snapshot) (bool, error) {
		if s.Busy == nil || *s.Busy != t.native {
			return false, status.Error(codes.Aborted, "native admission changed")
		}
		if d.blocked(s, target, group) {
			return false, status.Error(codes.FailedPrecondition, "source has an active composite use")
		}
		return false, nil
	})
}
