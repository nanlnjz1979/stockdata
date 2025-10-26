#!/usr/bin/env python
import os
import sys


def main():
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'stockserver.settings')
    # 兼容环境中旧版第三方 argparse 覆盖标准库导致的 allow_abbrev 报错
    try:
        import argparse
        _orig_init = argparse.ArgumentParser.__init__
        def _patched_init(self, *args, **kwargs):
            kwargs.pop('allow_abbrev', None)
            return _orig_init(self, *args, **kwargs)
        argparse.ArgumentParser.__init__ = _patched_init
    except Exception:
        pass
    from django.core.management import execute_from_command_line
    execute_from_command_line(sys.argv)


if __name__ == '__main__':
    main()