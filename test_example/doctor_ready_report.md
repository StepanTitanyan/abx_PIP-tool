<div align="center">

# ab doctor report

**✅ Ready for analysis**


No major issues detected.


**Summary:** 0 errors, 0 warnings, 4 info.


---

</div>


## INFO

### INTEGRITY_SUMMARY

- **Count:** 650


> **Finding**

> Rows=650, unique_users=650.


> **Explanation & next steps**

> Dataset summary
> • What it means: Basic dataset size summary.
> • Why it matters: Quick sanity check that the file has expected number of rows/users.
> • How to fix: None needed; use it to confirm input/output look reasonable.

> What it means: Basic dataset size summary.
> Why it matters: Quick sanity check that the file has expected number of rows/users.
> Diagnostics:
>   - n_rows=650
>   - n_users=650

### VARIANT_COUNTS

- **Count:** 2


> **Finding**

> Variant counts (users per variant).


> **Explanation & next steps**

> Variant counts
> • What it means: Number of users per variant.
> • Why it matters: Helps spot tiny arms, imbalance, or missing assignment.
> • How to fix: If unexpected: check assignment logic, exposure join, or filtering in conversion.

> What it means: Number of users per variant.
> Why it matters: Helps spot tiny arms, imbalance, or missing assignment.
> Diagnostics:
>   - n_users=650


<details>
<summary><b>Examples</b> (click to expand)</summary>


| variant | n_users | pct |
| --- | --- | --- |
| treatment | 331 | 50.9% |
| control | 319 | 49.1% |


</details>

### MISSINGNESS_SUMMARY

- **Count:** 7


> **Finding**

> Missingness summary across metrics.


> **Explanation & next steps**

> Missingness summary table
> • What it means: Overview of missing rates per metric and imbalance gaps.
> • Why it matters: Lets you quickly see which metrics are broken or biased.
> • How to fix: Use it to decide where to debug first.

> What it means: Overview of missing rates per metric and imbalance gaps.
> Why it matters: Lets you quickly see which metrics are broken or biased.


<details>
<summary><b>Examples</b> (click to expand)</summary>


| metric | overall_missing | worst_variant | best_variant | gap_pp |
| --- | --- | --- | --- | --- |
| conversion | 0/650 (0.0%) | control (0.0%) | control (0.0%) | 0.0 pp |
| purchases | 0/650 (0.0%) | control (0.0%) | control (0.0%) | 0.0 pp |
| revenue | 0/650 (0.0%) | control (0.0%) | control (0.0%) | 0.0 pp |
| refunds | 0/650 (0.0%) | control (0.0%) | control (0.0%) | 0.0 pp |
| refund_amount | 0/650 (0.0%) | control (0.0%) | control (0.0%) | 0.0 pp |
| clicks | 0/650 (0.0%) | control (0.0%) | control (0.0%) | 0.0 pp |
| views | 0/650 (0.0%) | control (0.0%) | control (0.0%) | 0.0 pp |


</details>

### METRICS_SUMMARY

- **Count:** 7


> **Finding**

> Metrics dtype/cast/quality summary (helps quickly spot broken columns).


> **Explanation & next steps**

> Metrics quality summary
> • What it means: Overview of dtype, missingness, cast failures, and constant metrics.
> • Why it matters: Helps you quickly identify broken metric columns.
> • How to fix: Use it to debug the highest-risk metrics first.

> What it means: Overview of dtype, missingness, cast failures, and constant metrics.
> Why it matters: Helps you quickly identify broken metric columns.


<details>
<summary><b>Examples</b> (click to expand)</summary>


| metric | dtype | missing | bad_numeric_cast | bad_cast_rate | nonfinite | constant |
| --- | --- | --- | --- | --- | --- | --- |
| conversion | int64 | 0 | 0 | 0.0% | 0 | 0 |
| purchases | int64 | 0 | 0 | 0.0% | 0 | 0 |
| revenue | float64 | 0 | 0 | 0.0% | 0 | 0 |
| refunds | int64 | 0 | 0 | 0.0% | 0 | 0 |
| refund_amount | float64 | 0 | 0 | 0.0% | 0 | 0 |
| clicks | int64 | 0 | 0 | 0.0% | 0 | 0 |
| views | int64 | 0 | 0 | 0.0% | 0 | 0 |


</details>
