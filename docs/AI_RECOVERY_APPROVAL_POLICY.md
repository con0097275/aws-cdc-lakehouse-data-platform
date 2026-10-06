# AI RECOVERY — POLICY AND APPROVAL

Source: `ai/reliability/policy.py`, `ai/reliability/control.py` · phase **AIGR6**

## 1. Three outcomes

`AUTO_EXECUTE_ALLOWED` · `APPROVAL_REQUIRED` · `BLOCKED`

Any single failing condition downgrades. Nothing upgrades.

## 2. BLOCKED — no approval can make these safe

| condition | reason |
|---|---|
| root cause `UNKNOWN` | |
| `TRANSFORM_LOGIC_DEFECT` | rerunning the same code reproduces the same wrong answer |
| `DQ_RULE_DEFECT` | the data may be correct |
| source correction pending | the platform must not write the source |
| unbounded scope | "rebuild everything" is the absence of a scope |
| column scope on non-validated lineage | the descendants it omits never get rebuilt |

## 3. APPROVAL_REQUIRED

Non-dev environment (always) · key count over limit · date count over limit · descendant
job count over limit · cost class over limit · **any prior attempt** · lineage only
`DECLARED` · caller holds no execute role.

The prior-attempt rule matters: a second *automatic* attempt at something that already
failed automatically is how one defect becomes a loop with a bill.

## 4. Limits are not negotiable at runtime

`PolicyLimits` defaults are lab-shaped and production overrides tighten them. A limit a
caller can raise is not a limit, so nothing loosens them at call time.

## 5. The approval record

`approval_id · plan_id · plan_hash · approver_identity · approval_scope · environment ·
approved_at · expires_at` (4h).

Invalid if: different plan, **changed hash**, different environment, or expired. A plan edit
therefore invalidates approval by construction rather than by policy.

## 6. A prompt phrase is never an approval

*"repair it"* means **build a recovery plan**. Execution additionally requires a policy
outcome that permits it and, in production, a captured approval from an identity holding
`APPROVE_PROD_RECOVERY`. An agent identity is refused outright.
