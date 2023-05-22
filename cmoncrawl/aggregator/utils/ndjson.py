import json


class Decoder(json.JSONDecoder):
    def decode(self, s: str, *args, **kwargs):
        lines = f"[{','.join(s.splitlines())}]"
        return super(Decoder, self).decode(lines, *args, **kwargs)
