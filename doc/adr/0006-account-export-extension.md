# 6. Account Export Extension to simplify imports

Date: 2020-07-01
Author: @mh

## Status

Proposed

## Context

The goal is to avoid tokens in situations where the account id can be part of the imported subject.
This is done by clamping the id of the importing account to the subject.

Where this is possible we hope to gain the following advantages:
1. Not depend on a running process to issue token prior to adding an import.
2. Eliminating the token issuer and the need to write and operate the associated process.
3. This moves token versioning issues away from the token issuer to the importers tooling.

This solution aims to reduce the friction associated with private imports between authorized accounts.
It does NOT replace tokens. 

## Configuring the simplified export 

This applies primarily to JWT but can be used in regular configuration as well.
Exports have an optional field called `account_token_position`.
It's value denotes the wildcard token inside the subject of the exported stream or service.
Position starts at 1 to make 0 the disabled value.

An exporting account has the following configuration. 
```
ABRWGT5FREDB5ZVVG5QP6HMUPVWJF5J4BHFGUUAUY3Y6OZGIFNX5YKC5 {
        exports [{
            stream "some.subject.*"
            account_token_position 3
        },{
            service "other.subject.*"
            account_token_position 3
        }]
    }
```

An importing account has the matching configuration:
``` 
ADY4IBKZZ7VTHGO5SBGUJ6BK3WMHTOQU347M7MUFD5J632EKTQCKPZRS {
    imports [{
        stream {
            account "ABRWGT5FREDB5ZVVG5QP6HMUPVWJF5J4BHFGUUAUY3Y6OZGIFNX5YKC5"
            subject "some.subject.ADY4IBKZZ7VTHGO5SBGUJ6BK3WMHTOQU347M7MUFD5J632EKTQCKPZRS"
        }},{
        service { 
            account "ABRWGT5FREDB5ZVVG5QP6HMUPVWJF5J4BHFGUUAUY3Y6OZGIFNX5YKC5"
            subject "other.subject.ADY4IBKZZ7VTHGO5SBGUJ6BK3WMHTOQU347M7MUFD5J632EKTQCKPZRS"
        }
        to "other.subject"
    }] 
}
```

## nats-server runtime behavior

As is currently the case for all forms of import/export, both account jwt are downloaded.
The server checks if the importing subject matches the export. 
It also checks if the importing account's id matches the n-th subject token specified by the exporting accounts `account_token_position`.
This ensures that no other account can publish/subscribe to that particular subject.

## Tooling changes

Tooling will be modified to store and specify `account_token_position`.
Upon adding an import, `nsc` will also auto complete the importing account's id to avoid unnecessary typing.

## Difference to tokens

This relies on the nats-server ensuring that the id of the importing account is part of the subject.
As such, that part of the subject can not be picked arbitrarily.

Tokens are issued for an account and thus the ability to present a token authorizes access.
For streams this does not matter. Messages just wouldn't be published to that subject.
Services can receive requests from every valid account that has correct imports.
If the service can verify authorization by account id inline, do not respond if that check fails.
Meaning, as a difference to tokens, the decision to process a request has to happen inline.

## Initial use cases

Especially system-wide services can be simplified with this.

1. Automatic imports as inserted by ngs can be phased out and wouldn't have to be maintained for JWT v2.
2. Account specific system services and streams can be imported this was as well.

In both cases, `nsc` could suggest to insert them.
