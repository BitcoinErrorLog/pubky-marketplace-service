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
    healthcheckPath: "/health",
    healthcheckTimeout: 120,
    restartPolicyType: "ON_FAILURE" as const,
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
  PAYPAL_IPN_VERIFY_URL: preserve(),
  SHIPPO_API_BASE: preserve(),
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
