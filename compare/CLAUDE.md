# Goal
- Compare a golfer's range session with prior range sessions, and provide suggestions for the golfer's next practice session. This data will be provided via a comma-delimited string or file

# Context
- Data is tracked using Flightscope, a radar-based golf launch monitor
- All shots use range balls, but balls may differ depending on the range location or ball batches
- is_recent_session is a boolean that distinguishes the two groups to be compared

# Metrics
- Metric definitions can be found in @../../database_assets/metric_definitions.csv
- CSV data will have two summary metrics for each metric: one for the mean (suffixed by "_avg"), and one for the standard deviation (suffixed by "_stdev")


# Output
- Highlight only the key metrics that either a) are notably different, and whether the trend is encouraging or not or b) if provided, are associated with what the golfer was working on during the session
- Prioritize recommendations for Club-based metrics, because this is what the golfer can influence. Mention Flight-based metrics secondarily if there is a common association between a swing improvement and distance improvement.
- Return the values in Quarto Markdown format
- After each item, generate a Quarto code block in Python to visualize a comparison between the two groups. Assume Polars and Seaborn are used.
- Example: The golfer was successful in shallowing the club during the downswing, as evidenced by a 0.7 degree reduction in Angle of Attack and 0.5 degree reduction in vertical swing plane, resulting in a more neutral club path (0.5 average) and tighter dispersion (8 -> 6 yard change in standard deviation of side distance). Continue working on [conscious swing thought], and also consider [common swing thought for fixing swing flaw], which [reasoning].
- Write the contents to @reports/[input_filename]_report.qmd

# Guidelines
* Keep recommendations concise. This text will be in a slide deck, so the text must be consumable by an average reader
