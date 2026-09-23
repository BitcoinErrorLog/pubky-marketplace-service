import { defineRailway, image, preserve, project, service } from "railway/iac";

// Last resort for a per-service CaC repo. Prefer one .railway file for the
// project and drop this if you later combine services into that file.
export const partial = "marketplace-service";

const STAGING_PROJECT_ID = "c991d768-4a3c-42ea-b5ed-eaa22d4916ed";
const STAGING_ENVIRONMENT_ID = "c67a6435-bb23-453b-9169-764bfa0312e1";
const PRODUCTION_PROJECT_ID = "75faa4fe-466c-4277-977f-1d8e4e31df8c";
const PRODUCTION_ENVIRONMENT_ID = "404919ad-fb95-4621-9f45-b7f993dfa8ae";

// `image()` takes a string only. `preserve()` is a variable value, not a
// source. Do not read this from process.env: a 0/0/0 plan must be a property
// of git, not of the operator's shell. The release train MUST bump each
// constant in the same PR as the IMAGE connect for that seat.
const LIVE_IMAGE =
  "ghcr.io/bitcoinerrorlog/pubky-marketplace-service@sha256:af9a5e1648beef813d7a0540ec3874755e91e6a853731348e3d811ef66e820ad";
const STAGING_IMAGE = LIVE_IMAGE;
const PRODUCTION_IMAGE = LIVE_IMAGE;

const sharedDeploy = {
  healthcheckPath: "/ready",
  healthcheckTimeout: 120,
  overlapSeconds: 60,
  drainingSeconds: 15,
};

const sharedEnv = {
  ALLOWED_ORIGINS: preserve(),
  BITCOIN_PAYMENT_WINDOW_SECONDS: preserve(),
  CHECKOUT_HOLD_WINDOW_SECONDS: preserve(),
  FIAT_PAYMENT_WINDOW_SECONDS: preserve(),
  ATTESTOR_ORDER_SALT: preserve(),
  ATTESTOR_SECRET_KEY: preserve(),
  DATABASE_URL: preserve(),
  HOMESERVER_URL: preserve(),
  LOCKS_BUNDLE_ENCRYPTION_KEY: preserve(),
  LOCKS_LOOKUP_HMAC_KEY: preserve(),
  LOCKS_SERVER_URL: preserve(),
  PAYKIT_REQUEST_SIGNING_KEY: preserve(),
  PAYKIT_SERVER_URL: preserve(),
  PICKUP_DETAILS_ENCRYPTION_KEY: preserve(),
  PUBLIC_APP_ORIGIN: preserve(),
  PUBLIC_SERVICE_ORIGIN: preserve(),
  REFUSAL_AUDIT_BACKUP_EXPIRY_ATTESTED: preserve(),
  REFUSAL_AUDIT_DATABASE_URL: preserve(),
  REFUSAL_AUDIT_HMAC_KEY_EPOCH: preserve(),
  REFUSAL_AUDIT_HMAC_PREVIOUS_KEY_EPOCH: preserve(),
  REFUSAL_AUDIT_HMAC_PREVIOUS_ROOT_B64: preserve(),
  REFUSAL_AUDIT_HMAC_ROOT_B64: preserve(),
  REFUSAL_AUDIT_REPLICA_EXPIRY_ATTESTED: preserve(),
  REFUSAL_AUDIT_RESIDUAL_RISK_ACCEPTED: preserve(),
  REFUSAL_AUDIT_RETENTION_DATABASE_URL: preserve(),
  RUST_LOG: preserve(),
  SANDBOX_PAYMENTS_ENABLED: preserve(),
  STRIPE_KEY_ENCRYPTION_KEY: preserve(),
};

const grantAndGatewayEnv = {
  GRANT_FLOW_ENCRYPTION_KEY_B64: preserve(),
  GRANT_FLOW_KEY_EPOCH: preserve(),
  GRANT_RESULT_HMAC_KEY_EPOCH: preserve(),
  GRANT_RESULT_HMAC_ROOT_B64: preserve(),
  MARKETPLACE_GRANT_CLIENT_ID: preserve(),
  MARKETPLACE_GRANT_CREATE_PER_IP_PER_MINUTE: preserve(),
  MARKETPLACE_GRANT_CREATE_PER_PUBKY_PER_MINUTE: preserve(),
  MARKETPLACE_GRANT_FLOW_ENABLED: preserve(),
  MARKETPLACE_GRANT_FLOW_TTL_SECONDS: preserve(),
  MARKETPLACE_GRANT_MAX_LIVE_FLOWS: preserve(),
  MARKETPLACE_GRANT_REAPER_BATCH_SIZE: preserve(),
  MARKETPLACE_GRANT_RELAY_POLL_MILLISECONDS: preserve(),
  MARKETPLACE_GRANT_RELAY_URL: preserve(),
  MARKETPLACE_GRANT_RESULT_PER_PRINCIPAL_PER_MINUTE: preserve(),
  MARKETPLACE_GRANT_STATUS_PER_FLOW_PER_MINUTE: preserve(),
  MARKETPLACE_GRANT_TERMINAL_RETENTION_SECONDS: preserve(),
  MARKETPLACE_GRANT_VERIFY_LEASE_SECONDS: preserve(),
  MARKETPLACE_GRANT_WORKER_BATCH_SIZE: preserve(),
  PAYPAL_IPN_VERIFY_URL: preserve(),
  SHIPPO_API_BASE: preserve(),
  SHOP_BFF_REQUEST_VERIFYING_KEYS_JSON: preserve(),
  SHOP_GRANT_ASSERTION_ISSUER: preserve(),
  SHOP_GRANT_ASSERTION_VERIFYING_KEYS_JSON: preserve(),
  STRIPE_API_BASE: preserve(),
};

const stagingEnv = {
  ...sharedEnv,
  ...grantAndGatewayEnv,
  AUTH_SESSION_TTL_SECONDS: preserve(),
};

const productionEnv = {
  ...sharedEnv,
  ...grantAndGatewayEnv,
};

function resolvedProjectId(ctx: { projectId?: string }): string | undefined {
  return ctx.projectId ?? process.env.RAILWAY_PROJECT_ID;
}

function resolvedEnvironmentId(ctx: { environmentId?: string }): string | undefined {
  return ctx.environmentId ?? process.env.RAILWAY_ENVIRONMENT_ID;
}

export default defineRailway((ctx) => {
  const projectId = resolvedProjectId(ctx);
  const environmentId = resolvedEnvironmentId(ctx);
  const isProduction = projectId === PRODUCTION_PROJECT_ID;
  const isStaging = projectId === STAGING_PROJECT_ID;
  if (!isProduction && !isStaging) {
    throw new Error(
      `Unknown Railway project ${projectId ?? "(none)"}. This file covers pubky-marketplace-staging (${STAGING_PROJECT_ID}) and pubky-marketplace-production (${PRODUCTION_PROJECT_ID}).`,
    );
  }
  if (isStaging && environmentId && environmentId !== STAGING_ENVIRONMENT_ID) {
    throw new Error(
      `Refuse to evaluate the staging graph against environment ${environmentId}. Set RAILWAY_ENVIRONMENT_ID=${STAGING_ENVIRONMENT_ID}.`,
    );
  }
  if (isProduction && environmentId && environmentId !== PRODUCTION_ENVIRONMENT_ID) {
    throw new Error(
      `Refuse to evaluate the production graph against environment ${environmentId}. Set RAILWAY_ENVIRONMENT_ID=${PRODUCTION_ENVIRONMENT_ID}.`,
    );
  }

  const marketplace_service = service("marketplace-service", {
    source: image(isStaging ? STAGING_IMAGE : PRODUCTION_IMAGE),
    // Live production still records builder DOCKERFILE (legacy toml).
    // Omitting it plans `build.builder DOCKERFILE → null`. Staging live has
    // no builder; declaring one would add it.
    ...(isProduction ? { build: { builder: "DOCKERFILE" as const } } : {}),
    deploy: sharedDeploy,
    env: isStaging ? stagingEnv : productionEnv,
    // Staging custom host is CLI-attached. Omitting this list deletes it.
    // Generated *.up.railway.app stays CLI.
    domains: isStaging ? ["staging-api.pubky.app"] : [],
  });

  return project(isStaging ? "pubky-marketplace-staging" : "pubky-marketplace-production", {
    resources: [marketplace_service],
  });
});
