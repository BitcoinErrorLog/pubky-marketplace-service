import { defineRailway, preserve, project, service } from "railway/iac";

// Last resort for a per-service CaC repo. Prefer one .railway file for the
// project and drop this if you later combine services into that file.
export const partial = "marketplace-service";

export default defineRailway(() => {
  const marketplace_service = service("marketplace-service", {
    build: {
      builder: "DOCKERFILE",
      dockerfilePath: "Dockerfile",
    },
    deploy: {
      healthcheckPath: "/health",
      healthcheckTimeout: 120,
      restartPolicyType: "ON_FAILURE",
    },
    env: {
      ALLOWED_ORIGINS: preserve(),
      ATTESTOR_ORDER_SALT: preserve(),
      ATTESTOR_SECRET_KEY: preserve(),
      AUTH_SESSION_TTL_SECONDS: preserve(),
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
    },
  });
  return project("pubky-marketplace-staging", {
    resources: [marketplace_service],
  });
});
