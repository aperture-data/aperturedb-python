import copy

SENSITIVE_KEYS = {"refresh_token", "session_token", "token", "access_token"}


def censor_tokens(data):
    """
    Recursively redact sensitive token fields in a dictionary or list.
    """
    def _censor(obj):
        if isinstance(obj, dict):
            for k, v in obj.items():
                if isinstance(k, str) and k.lower() in SENSITIVE_KEYS:
                    if isinstance(v, str):
                        parts = v.split("_", 1)
                        if len(parts) == 2:
                            prefix = parts[0] + "_"
                            token = parts[1]
                        else:
                            prefix = ""
                            token = v

                        if len(token) > 8:
                            obj[k] = prefix + token[:4] + "..." + token[-4:]
                        elif len(v) > 0:
                            obj[k] = prefix + "..."
                else:
                    _censor(v)
        elif isinstance(obj, list):
            for item in obj:
                _censor(item)

    censored = copy.deepcopy(data)
    _censor(censored)
    return censored
