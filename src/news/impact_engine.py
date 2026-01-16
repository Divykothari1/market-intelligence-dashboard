def generate_impact_explanation(
    sentiment: str,
    confidence: int,
    market_regime: str,
    signal: str
) -> str:
    """
    Generate human-readable impact explanation
    """

    # Base logic
    if sentiment == "Positive" and signal == "Bullish":
        return (
            "This news aligns positively with the current market trend. "
            "Positive developments combined with bullish technical signals "
            "increase the probability of continued upward movement."
        )

    if sentiment == "Negative" and signal == "Bearish":
        return (
            "Negative news reinforces the existing bearish trend. "
            "This may increase downside risk unless strong buying support emerges."
        )

    if sentiment == "Positive" and signal in ["Neutral", "Bearish"]:
        return (
            "While the news sentiment is positive, the price trend has not yet confirmed it. "
            "The stock may need further confirmation before a sustained move."
        )

    if sentiment == "Negative" and signal in ["Neutral", "Bullish"]:
        return (
            "Despite negative news, the stock is technically strong. "
            "If fundamentals dominate, the impact may be limited in the short term."
        )

    return (
        "The news impact appears mixed. Market participants may wait for further clarity "
        "before taking decisive positions."
    )
