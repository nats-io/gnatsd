# 5. JWT Authentication Extensibility

Date: 2020-07-01

## Status

Proposed.


## Context

We wish to have the option to, in future, extend or change the authentication
flow in a way which clients can safely handle.  This architecture design note
provides for a check which clients can safely add now, to be compatible with
such future changes and to be proof against certain attacks.

Upon connection, in classic mode the NATS server sends an `INFO` line with
various fields; the NATS client processes that, performs a TLS upgrade if
appropriate, and then sends a `CONNECT` line with various fields.
Some protocol variations adjust this (for websockets, for example) but this is
the core flow which all normal NATS protocol clients are expected to support.

At present, if a server supports JWT authentication then it generates a strong
random nonce, presented as the `nonce` field of the `INFO` line.  If a client
wishes to authenticate with a JWT, then it takes the bytes of that field, and
without any decoding signs them with the private key associated with the jwt;
the resulting signature is base64-encoded; then in the `CONNECT` line the
client includes two fields: `jwt` containing their JWT, and `sig` which
contains the base64-encoded result of signing the nonce.

This protects the server against replay attacks, since a strongly random nonce
is chosen each time.  It provides no inherent protection against
man-in-the-middle attacks, relying instead upon TLS checks.  The server can
enforce mutual-TLS, but not all deployment scenarios enforce this.

A well-behaved client with a competent TLS stack, configured to only proceed
with TLS, will verify the NATS server's certificate before proceeding, so will
only send the signature to a server which can present a certificate that
client expects.

In the IoT space, and in some programming languages, the TLS libraries don't
necessarily actually perform full certificate verification.  While there are
limits to how much "reinventing the wheel" should be done here, there is
established precedence in authentication protocols for being proof against
such scenarios.

One notable caveat here is the "enterprise SSL inspection" man-in-the-middle,
which presents a certificate which the client expects.  In practice, NATS will
not get through any such device which does not explicitly support NATS,
because such boxes primarily target TLS-on-connect and then add
protocol-specific workarounds for all other scenarios.


## Future

This ADR does not mandate one particular future.  Anything proposed here is a
scenario we wish to be able to handle.

Anything which has us sign "something different" faces the problem that a
man-in-the-middle server can present that "something different" to the client
as the nonce.  The MitM can take the real NATS server's nonce, perform the
construction required for a future flow, present the result to its own nonce
field, get the signed response and send that on.

The guard against that is to have clients refuse **now** to sign certain nonce
patterns, so that in the future they can not be tricked into signing something
else.  There is always the risk of ancient clients signing anyway, but the
sooner we establish a conformance baseline of not doing so, the smaller that
risk becomes and the easier it is for people deploying NATS clients to just
not provide valid JWT credentials to applications which don't do this check.

### One Possible Future

A pattern used in some authentication protocols is to use "channel binding" to
associate some property of the connection with the data being signed in
authentication, so that the same data sent from client-to-MitM would not be
valid when sent from MitM-to-server.

With TLS, this involves deriving some collision-resistant identifier from the
current TLS context; the first TLS channel binding approach failed this
collision-resistance requirement, but later versions have so far not been seen
to fail.

The NATS client would then have two pieces of data, a channel binding and the
server nonce, to combine together in some pattern before signing and sending
it on, together with some signal to the server that this pattern is being used
(structure of `sig` field, using another field, whatever).

Such a deployment would have to be optional, either at server or JWT level, to
handle connections through forced proxies.  People operating a NATS server
which does not expect connections to go through such an intermediary should be
able to configure their servers to reject authentication from JWTs known to be
issued to devices without adequate TLS verification, unless that
authentication is directly upon the channel without an intermediary.


### Other Futures

This ADR assumes that we will be combining two pieces of data together to sign
and that we don't want to let a MitM present the entire result as the nonce.

Shifting norms for best current security practice might require us to move
away from simple signing of the nonce and any check which clients can perform
now to aid in that may help extend the deployable lifetime of codebases
written today into that future.  Future-proofing is impossible in the general
case but that should not prevent reasonable accommodation now for sufficiently
plausible futures.

An impediment to any future which requires signing "something else" is that
the "something else" can be provided today in a nonce field and a compliant
client will sign it.

Moving the signature to another field instead of `sig` might help with
protocol handling but provides no security benefit.  A MitM can present
todays' clients with a `nonce` consisting of whatever needs to be signed, take
the `sig` field from the compliant client, and put that data into whatever
field the legitimate future server expects.

Our core problem requiring action now is that we will currently sign anything,
without structure, so future structured signatures can be coerced from
today's unstructured signers.


## Proposal

If a NATS client refuses to sign nonces which match certain patterns, then we
can use such a pattern in any future authentication scheme.

There's an existing pattern in NATS of constructing JSON objects and
presenting those, sometimes inside JWT.  A JSON object will start with `{`,
0x7B `LEFT CURLY BRACKET`.

As an implementation detail, the current nats-server codebase generates nonces
by using RFC 4648's unpadded URL encoding upon some data.  Until now, clients
have not been expected to enforce or expect this and we are not now
constraining the server to this particular choice.

The URL-safe base64 alphabet is:
`ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_`

We are constraining the server, now and forever under the current protocol,
with the rule: "the first octet/byte of the nonce must not be 0x7B".

For the current server code-base, no code changes are required except comments
to warn developers away from violating this constraint in the future.

For NATS clients, a new check is required: "If the first octet/byte of the
nonce is 0x7B then refuse to sign it and error out appropriately to represent
an inauthentic server".

In highly optimized clients, this additional check should be a couple of CPU
instructions without branching in the success case, once at connection
start-up, so is a negligible cost to pay for a hypothetical future
improvement.  We believe this cost is appropriate for all clients to pay.


## Decision

Pending.


## Consequences

Client libraries will need to implement this check.

This check should become part of a conformance test suite for various
languages' NATS clients.

Alternative server implementations will need to constrain their nonce
selection appropriately.

In future we will be better placed to adapt to shifting security requirements.

