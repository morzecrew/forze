---
title: Campaign results
icon: lucide/table
summary: The full detection-time protocol tables — N=300 per (mutant, strategy), exact intervals, and the p-hat-versus-PCT-bound analysis
---

# Campaign results

The full detection-time protocol over the misuse corpus: 300 independent campaigns per (mutant,
strategy), censored at a 2000-seed ceiling, strategies random / pct-d2 / pct-d3, false positives
measured on every known-correct control at 400 runs per cell. The whole dataset reproduces from
one master seed: `just dst-campaign-full` regenerates these tables, the p̂-versus-PCT-bound
section, and the charts (`just dst-campaign` runs the fast N=100 pilot). The
[detection-statistics page](detection-statistics.md) is the guided reading of this data.

--8<-- "dst/_generated/campaign_full.md"
