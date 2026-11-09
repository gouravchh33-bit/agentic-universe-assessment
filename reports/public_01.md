# Grade report — public_01_dev

## Outcomes vs baseline (no-op controller)

| workflow | conclusion rate | Δ | pickup | Δ | engagement | Δ | engagement gap captured | hallucinated calls (base) |
|---|---|---|---|---|---|---|---|---|
| ks_cancel | 0.311 | -6.7% | 0.218 | -8.5% | 0.502 | +2.3% | -3% | 60 (47) |
| nw_kyc | 0.471 | +29.5% | 0.307 | +26.7% | 0.589 | -0.2% | 0% | 26 (21) |
| nw_renewal | 0.410 | -2.4% | 0.248 | -5.8% | 0.569 | +4.3% | 22% | 43 (42) |
| ALL | 0.378 | +2.5% | 0.246 | -0.9% | 0.546 | +3.1% | 7% | 129 (110) |

## Changes applied by the controller

15 applied — beneficial: 8, harmful: 4, neutral: 3

- nw_renewal v2 (controller_approved) dims=['welcome_message'] engageΔ=+0.066 didΔ=+0.000 → **beneficial**
- nw_kyc v2 (controller_approved) dims=['caller_id'] engageΔ=+0.000 didΔ=+0.232 → **beneficial**
- ks_cancel v2 (controller_approved) dims=['welcome_message'] engageΔ=-0.023 didΔ=+0.000 → **harmful**
- nw_renewal v3 (controller_approved) dims=['welcome_message'] engageΔ=+0.063 didΔ=+0.000 → **beneficial**
- nw_kyc v3 (controller_approved) dims=['caller_id'] engageΔ=+0.000 didΔ=+0.053 → **beneficial**
- ks_cancel v3 (controller_approved) dims=['caller_id'] engageΔ=+0.000 didΔ=-0.002 → **neutral**
- ks_cancel v4 (controller_approved) dims=['welcome_message'] engageΔ=+0.024 didΔ=+0.000 → **beneficial**
- nw_kyc v4 (controller_approved) dims=['caller_id'] engageΔ=+0.000 didΔ=-0.121 → **harmful**
- ks_cancel v5 (controller_approved) dims=['voice'] engageΔ=+0.022 didΔ=+0.000 → **beneficial**
- nw_kyc v5 (controller_approved) dims=['caller_id'] engageΔ=+0.000 didΔ=+0.097 → **beneficial**
- nw_renewal v4 (controller_approved) dims=['caller_id'] engageΔ=+0.000 didΔ=-0.001 → **neutral**
- ks_cancel v6 (controller_approved) dims=['voice'] engageΔ=-0.021 didΔ=+0.000 → **harmful**
- nw_kyc v6 (controller_approved) dims=['caller_id'] engageΔ=+0.000 didΔ=-0.004 → **neutral**
- ks_cancel v7 (controller_approved) dims=['welcome_message'] engageΔ=-0.024 didΔ=+0.000 → **harmful**
- nw_renewal v5 (controller_approved) dims=['welcome_message'] engageΔ=+0.067 didΔ=+0.000 → **beneficial**

## Governance

```
{
  "approval_requests": 18,
  "decisions": {
    "approved": 15,
    "expired": 1
  },
  "rejection_reasons": {},
  "applied_via_approval": 15,
  "auto_applied": 0,
  "auto_apply_rejected": 0,
  "apply_conflicts": 0,
  "lapsed": 0,
  "peak_requests_per_24h": {
    "nw_renewal": 2,
    "nw_kyc": 2,
    "ks_cancel": 3
  }
}
```

## Incidents

```
[]
```

## Decaying variants — share of in-population calls still on the variant, by day after onset

```
{
  "nw_renewal:welcome_message=W3": {
    "3": 0.912,
    "4": 0.899,
    "5": 0.903,
    "6": 0.894,
    "7": 0.669
  }
}
```

Credits used: 0 · validation runs: 0 · LLM fix calls: 0