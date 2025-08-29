import polars as pl
from pathlib import Path

from proxy import CCProxy, CCProxyConfig


def main():
    df = pl.read_csv(f"{Path.cwd()}/data/sample.csv")
    urls = df["website"].unique()

    proxy = CCProxy(
        CCProxyConfig(
            agent_decl="cc (This is for academic research on entrepreneurial strategy, please contact at rylan@umd.edu)",
            target_months=["January", "February", "March"],
            year_range=range(2014, 2026),
        )
    )
    proxy.build_records(urls)
    proxy.save()


if __name__ == "__main__":
    main()
