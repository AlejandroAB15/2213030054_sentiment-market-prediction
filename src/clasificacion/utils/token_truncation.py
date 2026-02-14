from transformers import AutoTokenizer

class BetoTokenTruncator:
    #Trunca texto respetando el límite real de tokens (512)

    def __init__(
        self,
        model_name: str = "finiteautomata/beto-sentiment-analysis",
        max_tokens: int = 512
    ):
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.max_tokens = max_tokens

    def truncate_to_model_limit(self, text: str) -> str:
        """Aplica truncamiento por tokens, no por caracteres."""

        if not isinstance(text, str):
            return ""

        encoded = self.tokenizer(
            text,
            truncation=True,
            max_length=self.max_tokens
        )

        return self.tokenizer.decode(
            encoded["input_ids"],
            skip_special_tokens=True
        )
