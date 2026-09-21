import { defineRailway, preserve, project, service } from "railway/iac";

// Last resort for a per-service CaC repo. Prefer one .railway file for the
// project and drop this if you later combine services into that file.
export const partial = "marketplace-service";

const STAGING_PROJECT_ID = "c991d768-4a3c-42ea-b5ed-eaa22d4916ed";
const PRODUCTION_PROJECT_ID = "75faa4fe-466c-4277-977f-1d8e4e31df8c";

const sharedServiceConfig = {
  build: {
    builder: "DOCKERFILE" as const,
    dockerfilePath: "Dockerfile",
  },
  deploy: {
    healthcheckPath: "/ready",
    healthcheckTimeout: 120,
    restartPolicyType: "ON_FAILURE" as const,
    // Apply only after the 0037 image is SUCCESS. Dual-replica bound is
    // healthcheckTimeout 120 + overlapSeconds 60 + drainingSeconds 15.
    overlapSeconds: 60,
    drainingSeconds: 15,
  },
};

const sharedEnv = {
  ALLOWED_ORIGINS: preserve(),
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

const stagingEnv = {
  ...sharedEnv,
  AUTH_SESSION_TTL_SECONDS: preserve(),
};

const productionEnv = {
  ...sharedEnv,
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

export default defineRailway((ctx) => {
  const isProduction = ctx.projectId === PRODUCTION_PROJECT_ID;
  const isStaging = ctx.projectId === STAGING_PROJECT_ID;
  if (!isProduction && !isStaging) {
    throw new Error(
      `Unknown Railway project ${ctx.projectId ?? "(none)"}. This file covers pubky-marketplace-staging (${STAGING_PROJECT_ID}) and pubky-marketplace-production (${PRODUCTION_PROJECT_ID}).`,
    );
  }

  const marketplace_service = service("marketplace-service", {
    ...sharedServiceConfig,
    env: isProduction ? productionEnv : stagingEnv,
  });

  return project(
    isProduction ? "pubky-marketplace-production" : "pubky-marketplace-staging",
    { resources: [marketplace_service] },
  );
});
