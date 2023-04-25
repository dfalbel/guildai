# Pins remote

We first configure the pins remote:

    >>> txt = open("/tmp/pins-test-config.yml", "wt")
    >>> out = txt.write("""
    ... remotes:
    ...     guild-uat-pins:
    ...         type: pins
    ...         config:
    ...             board: folder
    ...             path: /tmp/pins-local-folder
    ...
    ... """)
    >>> txt.close()

Override `run` so we always use the configuration above

    >>> _run = run
    >>> def run(x):
    ...     _run(f"GUILD_CONFIG='/tmp/pins-test-config.yml' {x}")
    >>> run("rm -rf /tmp/pins-local-folder")
    <exit 0>

Now check specific configs

    >>> run("guild remote status guild-uat-pins")
    guild-uat-pins (Pins board ...) is available
    <exit 0>

    