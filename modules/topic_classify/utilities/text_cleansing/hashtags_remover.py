def hashtags_remover(text: str):
    """
    Remove hashtag from raw content
    """
    import re

    removed_text = re.sub(r"#\w+\s*", "", text);

    return removed_text;