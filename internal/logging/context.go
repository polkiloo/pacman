package logging

import (
	"context"
	"log/slog"
	"sort"
	"strings"
)

type correlationContextKey struct{}

type correlationFields map[string]string

func withCorrelationField(ctx context.Context, key, value string) context.Context {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ctx
	}

	current, _ := ctx.Value(correlationContextKey{}).(correlationFields)
	cloned := make(correlationFields, len(current)+1)
	for existingKey, existingValue := range current {
		cloned[existingKey] = existingValue
	}
	cloned[key] = trimmed

	return context.WithValue(ctx, correlationContextKey{}, cloned)
}

// WithRequestID attaches a request correlation identifier to the context.
func WithRequestID(ctx context.Context, requestID string) context.Context {
	return withCorrelationField(ctx, "request_id", requestID)
}

// WithPrincipalSubject attaches the authenticated principal subject.
func WithPrincipalSubject(ctx context.Context, subject string) context.Context {
	return withCorrelationField(ctx, "principal_subject", subject)
}

// WithPrincipalMechanism attaches the authentication mechanism.
func WithPrincipalMechanism(ctx context.Context, mechanism string) context.Context {
	return withCorrelationField(ctx, "principal_mechanism", mechanism)
}

// WithCluster attaches a cluster correlation field.
func WithCluster(ctx context.Context, clusterName string) context.Context {
	return withCorrelationField(ctx, "cluster", clusterName)
}

// WithNode attaches a node correlation field.
func WithNode(ctx context.Context, nodeName string) context.Context {
	return withCorrelationField(ctx, "node", nodeName)
}

// WithMember attaches a member correlation field.
func WithMember(ctx context.Context, memberName string) context.Context {
	return withCorrelationField(ctx, "member", memberName)
}

// WithOperation attaches operation identity fields.
func WithOperation(ctx context.Context, operationID, operationKind string) context.Context {
	ctx = withCorrelationField(ctx, "operation_id", operationID)
	return withCorrelationField(ctx, "operation_kind", operationKind)
}

// AttrsFromContext returns stable structured logging fields recorded on ctx.
func AttrsFromContext(ctx context.Context) []slog.Attr {
	fields, _ := ctx.Value(correlationContextKey{}).(correlationFields)
	if len(fields) == 0 {
		return nil
	}

	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	attributes := make([]slog.Attr, 0, len(keys))
	for _, key := range keys {
		attributes = append(attributes, slog.String(key, fields[key]))
	}

	return attributes
}
