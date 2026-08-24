-- Who verified the fiat payment that paid the order: 'processor' (Stripe,
-- restricted-key lookup), 'gateway' (PayPal, verified IPN), or 'seller'
-- (PayPal, manual confirm-received). NULL until a fiat payment is verified.
-- This drives the honest fiat_verification label: a PayPal order paid by a
-- verified IPN is 'gateway-notified', not 'seller-attested'.
ALTER TABLE orders ADD COLUMN fiat_verified_by TEXT;
