import { defineRailway, image, preserve, project, service } from "railway/iac";

export const partial = "marketplace-service";

const STAGING_PROJECT_ID = "c991d768-4a3c-42ea-b5ed-eaa22d4916ed";
const STAGING_ENVIRONMENT_ID = "c67a6435-bb23-453b-9169-764bfa0312e1";
// `image()` takes a string only. Bump in the same PR as every staging IMAGE connect.
const STAGING_IMAGE =
  "ghcr.io/bitcoinerrorlog/pubky-marketplace-service@sha256:beabefe1a05494308b33a8d584ce53c3d5722d0ee3088764680b6b56b232477f";

const stagingEnv = {
  ALLOWED_ORIGINS: preserve(),
  ATTESTOR_ORDER_SALT: preserve(),
  ATTESTOR_SECRET_KEY: preserve(),
  AUTH_SESSION_TTL_SECONDS: preserve(),
  BITCOIN_PAYMENT_WINDOW_SECONDS: preserve(),
  CHECKOUT_HOLD_WINDOW_SECONDS: preserve(),
  FIAT_PAYMENT_WINDOW_SECONDS: preserve(),
  DATABASE_URL: preserve(),
  DIGITAL_DELIVERY_ENCRYPTION_KEY: preserve(),
  GRANT_FLOW_ENCRYPTION_KEY_B64: preserve(),
  GRANT_FLOW_KEY_EPOCH: preserve(),
  GRANT_RESULT_HMAC_KEY_EPOCH: preserve(),
  GRANT_RESULT_HMAC_ROOT_B64: preserve(),
  HOMESERVER_URL: preserve(),
  LOCKS_BUNDLE_ENCRYPTION_KEY: preserve(),
  LOCKS_LOOKUP_HMAC_KEY: preserve(),
  LOCKS_SERVER_URL: preserve(),
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
  PAYKIT_REQUEST_SIGNING_KEY: preserve(),
  PAYKIT_SERVER_URL: preserve(),
  PAYPAL_IPN_VERIFY_URL: preserve(),
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
  SHIPPO_API_BASE: preserve(),
  SHOP_BFF_REQUEST_VERIFYING_KEYS_JSON: preserve(),
  SHOP_GRANT_ASSERTION_ISSUER: preserve(),
  SHOP_GRANT_ASSERTION_VERIFYING_KEYS_JSON: preserve(),
  STRIPE_API_BASE: preserve(),
  STRIPE_KEY_ENCRYPTION_KEY: preserve(),
};

export default defineRailway((ctx) => {
  const projectId = ctx.projectId ?? process.env.RAILWAY_PROJECT_ID;
  const environmentId = ctx.environmentId ?? process.env.RAILWAY_ENVIRONMENT_ID;
  if (projectId !== STAGING_PROJECT_ID) {
    throw new Error(
      `railway.staging.ts refuses project ${projectId ?? "(none)"}. Set RAILWAY_PROJECT_ID=${STAGING_PROJECT_ID} and RAILWAY_ENVIRONMENT_ID=${STAGING_ENVIRONMENT_ID}.`,
    );
  }
  if (environmentId !== STAGING_ENVIRONMENT_ID) {
    throw new Error(
      `railway.staging.ts refuses environment ${environmentId ?? "(none)"}. Set RAILWAY_ENVIRONMENT_ID=${STAGING_ENVIRONMENT_ID}.`,
    );
  }

  const marketplace_service = service("marketplace-service", {
    source: image(STAGING_IMAGE),
    deploy: {
      healthcheckPath: "/ready",
      healthcheckTimeout: 120,
      overlapSeconds: 60,
      drainingSeconds: 15,
    },
    env: stagingEnv,
    domains: ["staging-api.pubky.app"],
  });

  return project("pubky-marketplace-staging", { resources: [marketplace_service] });
});
