import json
import os

_CFG_PATH = os.path.join(os.getcwd(), 'queuectl_config.json')

_DEFAULTS = {
    'max_retries': 3,
    'backoff_base': 2,
    # default job timeout in seconds (0 or null means no timeout)
    'job_timeout': 0
}


def _load():
    if not os.path.exists(_CFG_PATH):
        return dict(_DEFAULTS)
    try:
        with open(_CFG_PATH, 'r') as f:
            d = json.load(f)
            out = dict(_DEFAULTS)
            out.update(d)
            return out
    except Exception:
        return dict(_DEFAULTS)


def _save(cfg):
    with open(_CFG_PATH, 'w') as f:
        json.dump(cfg, f)


def get_config(key):
    cfg = _load()
    return cfg.get(key, _DEFAULTS.get(key))


def set_config(key, value):
    cfg = _load()
    # try cast to int for numeric options
    if key in ('max_retries', 'backoff_base'):
        try:
            value = int(value)
        except Exception:
            raise ValueError('value must be integer')
    cfg[key] = value
    _save(cfg)
