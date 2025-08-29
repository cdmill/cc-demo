import polars as pl
from pathlib import Path

from proxy import CCProxy, CCProxyConfig


def main():
    df = pl.read_csv(f"{Path.cwd()}/data/sample.csv")
    urls = df["website"].unique()

    proxy = CCProxy(
        CCProxyConfig(
            agent_decl="cc-demo/0.0.1 (description <email>)",
            target_month="01",
        )
    )
    proxy.build_records(urls)
    proxy.save()


if __name__ == "__main__":
    main()
