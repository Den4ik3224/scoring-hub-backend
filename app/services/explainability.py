def classify_primary_driver(node_contributions: dict[str, float]) -> str:
    if not node_contributions:
        return "conversion-driven"

    buckets = {
        "conversion-driven": 0.0,
        "frequency-driven": 0.0,
        "basket-driven": 0.0,
        "margin-driven": 0.0,
    }

    for key, value in node_contributions.items():
        key_l = key.lower()
        abs_value = abs(float(value))
        if "conversion" in key_l or "funnel_step" in key_l or "step" in key_l:
            buckets["conversion-driven"] += abs_value
        elif "frequency" in key_l:
            buckets["frequency-driven"] += abs_value
        elif any(token in key_l for token in ("aoq", "aiv", "aov", "items")):
            buckets["basket-driven"] += abs_value
        elif any(token in key_l for token in ("fm_pct", "margin")):
            buckets["margin-driven"] += abs_value
        else:
            buckets["conversion-driven"] += abs_value

    return max(buckets.items(), key=lambda item: item[1])[0]


def build_summary_text(
    *,
    primary_driver: str,
    top_segment: str | None,
    top_screen: str | None,
    cannibalization_loss: float,
) -> str:
    segment_text = top_segment or "n/a"
    screen_text = top_screen or "n/a"
    return (
        f"Impact is mainly {primary_driver}. "
        f"Top contribution comes from segment `{segment_text}` on screen `{screen_text}`. "
        f"Cannibalization reduces net margin by {cannibalization_loss:,.2f}."
    )
