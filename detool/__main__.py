from functools import partial

import click

from .__about__ import __version__

st_bold = partial(click.style, bold=True)
st_bold_red = partial(click.style, bold=True, fg="red")
st_bold_green = partial(click.style, bold=True, fg="green")
echo = click.echo


@click.group()
def cli() -> None:
    """Main Tool CLI."""


@cli.command("version")
def version() -> None:
    echo(__version__)


if __name__ == "__main__":
    cli()
