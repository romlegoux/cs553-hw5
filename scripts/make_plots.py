#!/usr/bin/env python3
"""
make_plots.py -- turn the raw results of HW5 into the tables and figures of the report.

Usage (from the repository root):
    python3 scripts/make_plots.py [--results results] [--out report]

Inputs (all under --results, as written by the run-*.sh scripts):
    part2/cluster{A,B}/sorts.csv                 Table 4a (sort times)
    part2/cluster{A,B}/{mrsort,sparksort}_64GB_r1_<vm>.sar.gz, *.start, *.log
                                                 Figures 4b (resource usage during the 64GB sorts)
    part3/clusterB/pipeline.csv, part3/duckdb/duckdb.csv
                                                 Table 4c (pipeline stages)
    part3/clusterB/funnel_fw15.csv, top25_words_fw15.tsv,
    top_domains_fw15.csv, tokens_per_dump_fw15.csv
                                                 Table 4d (what the pipeline kept)
    part4e/clusterB/scaling_fw2.csv              Table 4e (scaling of B2)
    questions/q3, q8, q9                         Tables for Q1, Q3, Q8, Q9

Outputs (under --out):
    figures/*.png      one PNG per figure, ready to include in the report
    tables/*.md        Markdown tables (paste into the report, or convert with pandoc)
    tables/*.csv       the same tables as CSV
    sar_csv/*.csv      sar files converted to text (cache; delete to regenerate)

Dependencies: python3, pandas, matplotlib, and `sadf` (package sysstat) for Figure 4b.
    sudo apt install -y sysstat python3-pandas python3-matplotlib
The sar files are binary; `sadf` from sysstat 12.x (Ubuntu 24.04) reads them.
If `sadf` is missing, Figure 4b is skipped and everything else is still produced.

Conventions:
    * Every timing is the wall-clock time of the whole command (/usr/bin/time -f %e).
    * Tables report the MEAN of the repetitions; figures show the SPREAD as min-max error bars.
    * A run marked "(1 run)" was long enough (> 10 min) to be run only once.
    * All timestamps (sar, Hadoop and Spark logs, *.start files) are UTC.
"""

import argparse
import glob
import os
import re
import shutil
import subprocess
from datetime import datetime, timezone

import matplotlib
matplotlib.use("Agg")                      # no display needed: write PNG files only
import matplotlib.pyplot as plt
import pandas as pd

# --------------------------------------------------------------------------------------
# Style. Colors follow the ENTITY (a framework, a sort, a cluster) and keep the same
# meaning in every figure. They come from a palette checked for color-blind readers.
# --------------------------------------------------------------------------------------
BLUE, ORANGE, AQUA, GREEN, VIOLET = "#2a78d6", "#eb6834", "#1baf7a", "#008300", "#6250d6"
INK, INK2, GRID = "#0b0b0b", "#52514e", "#e4e3df"

SORT_COLOR = {"terasort": BLUE, "mrsort": ORANGE, "sparksort": AQUA}
SORT_NAME = {"terasort": "TeraSort", "mrsort": "MRSort", "sparksort": "SparkSort"}
FW_COLOR = {"mapreduce": ORANGE, "spark": AQUA, "duckdb": BLUE}
FW_NAME = {"mapreduce": "MapReduce", "spark": "Spark", "duckdb": "DuckDB"}
CLUSTER_COLOR = {"A": VIOLET, "B": GREEN}
CLUSTER_NAME = {"A": "A: 1 large VM", "B": "B: 8 small VMs"}
DATASETS = ["fw1", "fw8", "fw15"]
DS_NAME = {"fw1": "FW-1", "fw2": "FW-2", "fw4": "FW-4", "fw8": "FW-8", "fw15": "FW-15"}


def setup_style():
    plt.rcParams.update({
        "figure.dpi": 100, "savefig.dpi": 200, "savefig.bbox": "tight",
        "figure.facecolor": "white", "axes.facecolor": "white",
        "font.size": 9, "axes.titlesize": 10, "axes.labelsize": 9,
        "axes.edgecolor": INK2, "axes.labelcolor": INK, "text.color": INK,
        "xtick.color": INK2, "ytick.color": INK2,
        "axes.spines.top": False, "axes.spines.right": False,
        "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.6,
        "axes.axisbelow": True, "legend.frameon": False, "lines.linewidth": 2,
    })


def save(fig, out, name):
    path = os.path.join(out, "figures", name)
    fig.savefig(path)
    plt.close(fig)
    print("  figure ", path)


def write_table(df, out, name, title, note=None):
    """Write a table as Markdown (for the report) and as CSV."""
    base = os.path.join(out, "tables", name)
    df.to_csv(base + ".csv", index=False)
    with open(base + ".md", "w") as f:
        f.write(f"### {title}\n\n")
        f.write(df.to_markdown(index=False) if has_tabulate() else df.to_string(index=False))
        f.write("\n")
        if note:
            f.write(f"\n{note}\n")
    print("  table  ", base + ".md")


def has_tabulate():
    try:
        import tabulate  # noqa: F401  (pandas.to_markdown needs it)
        return True
    except ImportError:
        return False


def stats(df, keys, col="seconds"):
    """Mean, min, max and number of runs of `col` for each group of `keys`."""
    g = df.groupby(keys)[col]
    return g.agg(mean="mean", lo="min", hi="max", n="count").reset_index()


def fmt_time(mean, n):
    """'123.4' for a repeated run, '123.4 (1 run)' for a run done once."""
    return f"{mean:.1f}" if n > 1 else f"{mean:.1f} (1 run)"


def errbars(s):
    """Asymmetric min-max error bars for matplotlib, from a stats() frame."""
    return [(s["mean"] - s["lo"]).values, (s["hi"] - s["mean"]).values]


# ======================================================================================
# 4a. Sorting times and throughput
# ======================================================================================
def part_4a(res, out):
    print("4a: sorting")
    frames = [pd.read_csv(f) for f in sorted(glob.glob(f"{res}/part2/cluster*/sorts.csv"))]
    if not frames:
        print("  no sorts.csv found, skipped")
        return
    df = pd.concat(frames)
    bad = df[df["checksum_ok"] != "yes"]
    if len(bad):
        print("  WARNING: runs with a failed TeraValidate check:\n", bad)
    s = stats(df, ["cluster", "size_gb", "sort"])
    s["gbps"] = s["size_gb"] / s["mean"]          # 16GB means 16e9 bytes, as in the assignment

    # Table: one row per (cluster, size), one column per sort: time, then throughput.
    rows = []
    for (c, size), grp in s.groupby(["cluster", "size_gb"]):
        row = {"Cluster": CLUSTER_NAME[c], "Data": f"{size}GB"}
        for sort in SORT_NAME:
            r = grp[grp["sort"] == sort]
            if len(r):
                row[f"{SORT_NAME[sort]} (s)"] = fmt_time(r["mean"].iloc[0], r["n"].iloc[0])
        for sort in SORT_NAME:
            r = grp[grp["sort"] == sort]
            if len(r):
                row[f"{SORT_NAME[sort]} (GB/s)"] = f"{r['gbps'].iloc[0]:.3f}"
        rows.append(row)
    write_table(pd.DataFrame(rows), out, "table_4a_sorting", "Table 4a - Sorting time and throughput",
                "Mean of 3 runs unless marked. Throughput = data size (10^9 bytes) / mean time.")

    # Figure: time per size, one panel per cluster, same y scale so A and B compare directly.
    clusters = sorted(s["cluster"].unique())
    fig, axes = plt.subplots(1, len(clusters), figsize=(4.2 * len(clusters), 3.4), sharey=True)
    axes = [axes] if len(clusters) == 1 else axes
    sizes = sorted(s["size_gb"].unique())
    w = 0.26
    for ax, c in zip(axes, clusters):
        for k, sort in enumerate(SORT_NAME):
            r = s[(s["cluster"] == c) & (s["sort"] == sort)].set_index("size_gb").reindex(sizes)
            x = [i + (k - 1) * w for i in range(len(sizes))]
            ax.bar(x, r["mean"], w * 0.92, color=SORT_COLOR[sort], label=SORT_NAME[sort],
                   yerr=errbars(r), error_kw={"elinewidth": 1, "capsize": 2, "ecolor": INK2})
        ax.set_xticks(range(len(sizes)), [f"{z} GB" for z in sizes])
        ax.set_title(f"Cluster {CLUSTER_NAME[c]}")
        ax.grid(axis="x", visible=False)
    axes[0].set_ylabel("Time (s)")
    axes[0].legend(loc="upper left")
    fig.suptitle("Sorting time (mean, bars = min-max of 3 runs; 64 GB: 1 run)", fontsize=10)
    save(fig, out, "fig4a_sort_time.png")

    # Figure: throughput vs size, one panel per sort, A and B as two lines (useful for Q2).
    fig, axes = plt.subplots(1, 3, figsize=(10, 3.2), sharey=True)
    for ax, sort in zip(axes, SORT_NAME):
        for c in clusters:
            r = s[(s["cluster"] == c) & (s["sort"] == sort)].sort_values("size_gb")
            ax.plot(r["size_gb"], r["gbps"], "-o", ms=6, color=CLUSTER_COLOR[c], label=CLUSTER_NAME[c])
        ax.set_title(SORT_NAME[sort])
        ax.set_xticks(sizes, [f"{z} GB" for z in sizes])
    axes[0].set_ylim(0, s["gbps"].max() * 1.12)   # shared y axis: set the range once, from all the data
    axes[0].set_ylabel("Throughput (GB/s)")
    axes[0].legend(loc="lower left")
    save(fig, out, "fig4a_sort_throughput.png")


# ======================================================================================
# 4b. Resource usage during the 64GB sorts (sar)
# ======================================================================================
SADF_REPORTS = {"cpu": "-u", "mem": "-r", "disk": "-d", "net": "-n DEV"}
WHOLE_DISK = re.compile(r"^(sd[a-z]+|vd[a-z]+|nvme\d+n\d+|xvd[a-z]+)$")   # not partitions, not loops


def sadf_to_csv(sar_gz, report, cache_dir):
    """Convert one binary sar file to a DataFrame with `sadf -d`, caching the text output."""
    stem = os.path.basename(sar_gz).replace(".sar.gz", "")
    cache = os.path.join(cache_dir, f"{stem}_{report}.csv")
    if not os.path.exists(cache):
        raw = os.path.join(cache_dir, stem + ".sar")
        subprocess.run(f"zcat '{sar_gz}' > '{raw}'", shell=True, check=True)
        txt = subprocess.run(f"sadf -d '{raw}' -- {SADF_REPORTS[report]}", shell=True,
                             check=True, capture_output=True, text=True).stdout
        os.remove(raw)
        with open(cache, "w") as f:
            f.write(txt.lstrip("# "))           # the header line starts with "# "
    df = pd.read_csv(cache, sep=";")
    ts = pd.to_datetime(df["timestamp"].str.replace(" UTC", ""), utc=True)
    df["t"] = (ts - pd.Timestamp("1970-01-01", tz="UTC")) // pd.Timedelta(seconds=1)   # epoch seconds
    return df


def worker_series(sar_gz, cache_dir):
    """Per-second CPU %, memory GB, disk and network MB/s of ONE worker."""
    cpu = sadf_to_csv(sar_gz, "cpu", cache_dir)
    cpu = cpu[cpu["CPU"] == -1]                                   # -1 = all CPUs together
    cpu = pd.DataFrame({"t": cpu["t"], "cpu": 100 - cpu["%idle"]})

    mem = sadf_to_csv(sar_gz, "mem", cache_dir)                   # kbmemused excludes page cache
    mem = pd.DataFrame({"t": mem["t"], "mem_gb": mem["kbmemused"] / 1024**2})

    dsk = sadf_to_csv(sar_gz, "disk", cache_dir)
    dsk = dsk[dsk["DEV"].astype(str).str.match(WHOLE_DISK)]       # count each disk once
    dsk = dsk.groupby("t")[["rkB/s", "wkB/s"]].sum().reset_index()
    dsk = pd.DataFrame({"t": dsk["t"], "disk_r": dsk["rkB/s"] / 1024, "disk_w": dsk["wkB/s"] / 1024})

    net = sadf_to_csv(sar_gz, "net", cache_dir)
    net = net[net["IFACE"] != "lo"]                               # loopback is not the network
    net = net.groupby("t")[["rxkB/s", "txkB/s"]].sum().reset_index()
    net = pd.DataFrame({"t": net["t"], "net_rx": net["rxkB/s"] / 1024, "net_tx": net["txkB/s"] / 1024})

    return cpu.merge(mem, on="t").merge(dsk, on="t").merge(net, on="t")


def cluster_series(files, cache_dir):
    """Combine the workers: CPU is the mean (= % of all worker vCPUs), everything else the sum."""
    w = pd.concat([worker_series(f, cache_dir) for f in files])
    agg = {"cpu": "mean", "mem_gb": "sum", "disk_r": "sum", "disk_w": "sum", "net_rx": "sum", "net_tx": "sum"}
    counts = w.groupby("t").size()
    full = counts[counts == len(files)].index                     # seconds where every worker reported
    return w[w["t"].isin(full)].groupby("t").agg(agg).reset_index()


def mr_markers(log, start):
    """MapReduce: first time the console shows `map 100%` = end of the map phase."""
    with open(log, errors="replace") as f:
        for line in f:
            if "map 100%" in line:
                ts = datetime.strptime(line[:23], "%Y-%m-%d %H:%M:%S,%f").replace(tzinfo=timezone.utc)
                return [("map phase ends", ts.timestamp() - start)]
    return []


SPARK_STAGE = re.compile(r"^(\d\d/\d\d/\d\d \d\d:\d\d:\d\d) INFO DAGScheduler: (\w+Stage) (\d+) \((.*?)\) finished")


def spark_markers(log, start):
    """Spark: end of every stage, from the DAGScheduler lines of the console log."""
    marks = []
    with open(log, errors="replace") as f:
        for line in f:
            m = SPARK_STAGE.match(line)
            if not m:
                continue
            ts = datetime.strptime(m.group(1), "%y/%m/%d %H:%M:%S").replace(tzinfo=timezone.utc)
            kind, sid, what = m.group(2), m.group(3), m.group(4).split(" at ")[0]
            if kind == "ShuffleMapStage":
                label = f"stage {sid} ends: map side of the shuffle"
            elif "sortByKey" in what:
                label = f"stage {sid} ends: sampling the keys"
            elif "runJob" in what or "save" in what:
                label = f"stage {sid} ends: output written"
            else:
                label = f"stage {sid} ends: {what}"
            marks.append((label, ts.timestamp() - start))
    return marks


def part_4b(res, out):
    print("4b: resource usage of the 64GB sorts")
    if shutil.which("sadf") is None:
        print("  `sadf` not found (sudo apt install sysstat): Figure 4b skipped")
        return
    cache = os.path.join(out, "sar_csv")
    os.makedirs(cache, exist_ok=True)
    for cdir in sorted(glob.glob(f"{res}/part2/cluster*")):
        c = cdir[-1]
        times = pd.read_csv(f"{cdir}/sorts.csv")
        for sort in ("mrsort", "sparksort"):
            files = sorted(glob.glob(f"{cdir}/{sort}_64GB_r1_*.sar.gz"))
            if not files:
                continue
            start = int(open(f"{cdir}/{sort}_64GB_r1.start").read().split()[0])
            dur = times[(times["sort"] == sort) & (times["size_gb"] == 64) & (times["rep"] == 1)]["seconds"].iloc[0]
            log = f"{cdir}/{sort}_64GB_r1.log"
            marks = mr_markers(log, start) if sort == "mrsort" else spark_markers(log, start)

            d = cluster_series(files, cache)
            d["x"] = d["t"] - start
            d = d[(d["x"] >= -5) & (d["x"] <= dur + 15)]

            fig, axes = plt.subplots(4, 1, figsize=(8, 8.5), sharex=True)
            nw = len(files)
            axes[0].plot(d["x"], d["cpu"], color=BLUE, lw=1.2)
            axes[0].set_ylabel("CPU (%)")
            axes[0].set_ylim(0, 100)
            axes[1].plot(d["x"], d["mem_gb"], color=BLUE, lw=1.2)
            axes[1].set_ylabel("Memory used (GB)")
            axes[1].set_ylim(bottom=0)
            axes[2].plot(d["x"], d["disk_r"], color=BLUE, lw=1.2, label="read")
            axes[2].plot(d["x"], d["disk_w"], color=ORANGE, lw=1.2, label="write")
            axes[2].set_ylabel("Disk (MB/s)")
            axes[3].plot(d["x"], d["net_rx"], color=BLUE, lw=1.2, label="received")
            axes[3].plot(d["x"], d["net_tx"], color=ORANGE, lw=1.2, label="sent")
            axes[3].set_ylabel("Network (MB/s)")
            axes[3].set_xlabel("Time since submission (s)")
            for ax in axes[2:]:
                ax.legend(loc="upper left", bbox_to_anchor=(1.0, 1.0))   # outside the plot area
                ax.set_ylim(bottom=0)

            # Vertical markers: phase boundaries, then the end of the job (unless a stage
            # end already marks it). Labels sit ABOVE the top panel, on two staggered rows,
            # so they never cover the data.
            if not marks or dur - marks[-1][1] > 0.03 * dur:
                marks = marks + [("job ends", dur)]
            xmax = dur + 15
            for i, (label, x) in enumerate(marks):
                for ax in axes:
                    ax.axvline(x, color=INK2, lw=0.9, ls="--")
                ha = "right" if x > 0.8 * xmax else ("left" if x < 0.2 * xmax else "center")
                axes[0].text(x, 1.03 + 0.13 * (i % 2), label, transform=axes[0].get_xaxis_transform(),
                             ha=ha, va="bottom", fontsize=7.5, color=INK2)

            what = "mean over the 8 workers' 64 vCPUs" if nw > 1 else "the 64 vCPUs of the VM"
            fig.suptitle(f"{SORT_NAME[sort]}, 64 GB, cluster {CLUSTER_NAME[c]}\n"
                         f"CPU: {what}; memory, disk, network: sum over {nw} worker(s)", fontsize=9.5, y=0.995)
            fig.tight_layout(rect=(0, 0, 1, 0.955))
            save(fig, out, f"fig4b_{sort}_64GB_cluster{c}.png")


# ======================================================================================
# 4c. Pipeline stages on cluster B, and the DuckDB baseline
# ======================================================================================
ROWS_4C = [("B0", "spark", "Ingest"), ("B1", "mapreduce", "Word count"), ("B1", "spark", "Word count"),
           ("B1", "duckdb", "Word count"), ("B2", "mapreduce", "Filter + exact dedup"),
           ("B2", "spark", "Filter + exact dedup"), ("B2", "duckdb", "Filter + exact dedup"),
           ("B3", "spark", "Near-dup"), ("B4", "spark", "Report")]


def load_pipeline(res):
    p = pd.read_csv(f"{res}/part3/clusterB/pipeline.csv")
    dk = f"{res}/part3/duckdb/duckdb.csv"
    if os.path.exists(dk):
        d = pd.read_csv(dk)
        d = pd.DataFrame({"stage": d["stage"].str.upper(), "framework": "duckdb",
                          "dataset": d["dataset"], "rep": d["rep"], "seconds": d["seconds"]})
        p = pd.concat([p, d])
    return stats(p, ["stage", "framework", "dataset"])


def part_4c(res, out):
    print("4c: pipeline")
    s = load_pipeline(res)
    rows = []
    for stage, fw, name in ROWS_4C:
        row = {"Stage": f"{stage} {name}", "Framework": FW_NAME[fw]}
        for ds in DATASETS:
            r = s[(s["stage"] == stage) & (s["framework"] == fw) & (s["dataset"] == ds)]
            row[f"{DS_NAME[ds]} (s)"] = fmt_time(r["mean"].iloc[0], r["n"].iloc[0]) if len(r) else "-"
        rows.append(row)
    write_table(pd.DataFrame(rows), out, "table_4c_pipeline", "Table 4c - Pipeline stages (cluster B; DuckDB on the large VM)",
                "Mean of 3 runs unless marked. All Spark stages are written in Java (no PySpark).")

    # Figure: one panel per dataset, horizontal bars, colored by framework.
    fig, axes = plt.subplots(1, 3, figsize=(11, 3.8), sharey=True)
    labels = [f"{st} {name} ({FW_NAME[fw]})" for st, fw, name in ROWS_4C]   # never color alone
    for ax, ds in zip(axes, DATASETS):
        for i, (stage, fw, name) in enumerate(ROWS_4C):
            r = s[(s["stage"] == stage) & (s["framework"] == fw) & (s["dataset"] == ds)]
            if not len(r):
                continue
            ax.barh(i, r["mean"].iloc[0], 0.7, color=FW_COLOR[fw], xerr=errbars(r),
                    error_kw={"elinewidth": 1, "capsize": 2, "ecolor": INK2})
        ax.set_title(DS_NAME[ds])
        ax.set_xlabel("Time (s)")
        ax.grid(axis="y", visible=False)
    axes[0].set_yticks(range(len(labels)), labels)
    axes[0].invert_yaxis()
    handles = [plt.Rectangle((0, 0), 1, 1, color=FW_COLOR[f]) for f in FW_NAME]
    fig.legend(handles, FW_NAME.values(), loc="upper center", ncol=3, bbox_to_anchor=(0.55, 1.04))
    save(fig, out, "fig4c_pipeline.png")


# ======================================================================================
# 4d. What the pipeline kept (FW-15)
# ======================================================================================
def part_4d(res, out):
    print("4d: funnel, words, domains, snapshots (FW-15)")
    d = f"{res}/part3/clusterB"
    fun = pd.read_csv(f"{d}/funnel_fw15.csv")
    docs0, tok0 = fun["docs"].iloc[0], fun["tokens"].iloc[0]
    names = ["B0 Ingest (all of FW-15)", "B2 Quality filter", "B2 Exact deduplication", "B3 Near-dup removal"]
    table = pd.DataFrame({
        "After": names[:len(fun)],
        "Documents": fun["docs"].map("{:,}".format),
        "Documents kept (%)": (100 * fun["docs"] / docs0).map("{:.2f}".format),
        "Tokens": fun["tokens"].map("{:,}".format),
        "Tokens kept (%)": (100 * fun["tokens"] / tok0).map("{:.2f}".format),
    })
    write_table(table, out, "table_4d_funnel", "Table 4d - Funnel on FW-15")

    # Figure: what each stage REMOVED. The kept fraction stays above 98%, so bars of
    # "what is left" would all look identical; the removed amounts are what differs.
    removed_docs = -fun["docs"].diff().iloc[1:].values
    removed_tok = -fun["tokens"].diff().iloc[1:].values
    steps = ["Quality filter", "Exact dedup", "Near-dup (B3)"][:len(removed_docs)]
    fig, axes = plt.subplots(1, 2, figsize=(10, 2.8), sharey=True, gridspec_kw={"wspace": 0.15})
    for ax, vals, total, unit, title in ((axes[0], removed_docs, docs0, 1e3, "Documents removed (thousands)"),
                                         (axes[1], removed_tok, tok0, 1e6, "Tokens removed (millions)")):
        ax.barh(steps, vals / unit, 0.6, color=BLUE)
        for i, v in enumerate(vals):
            ax.annotate(f"{v / unit:,.1f}  ({100 * v / total:.2f}% of input)", (v / unit, i),
                        xytext=(4, 0), textcoords="offset points", va="center", fontsize=8, color=INK)
        ax.set_title(title)
        ax.set_xlim(0, max(vals / unit) * 1.9)
        ax.grid(axis="y", visible=False)
    axes[0].invert_yaxis()                      # once: the y axis is shared by both panels
    fig.suptitle(f"FW-15 funnel: {fun['docs'].iloc[-1]:,} of {docs0:,} documents and "
                 f"{100 * fun['tokens'].iloc[-1] / tok0:.2f}% of the tokens are kept", fontsize=10, y=1.03)
    save(fig, out, "fig4d_funnel.png")

    # Top 25 words (B1) and top 20 domains (B4).
    words = pd.read_csv(f"{d}/top25_words_fw15.tsv", sep="\t", header=None, names=["Word", "Count"])
    words.insert(0, "Rank", range(1, len(words) + 1))
    words["Count"] = words["Count"].map("{:,}".format)
    write_table(words, out, "table_4d_top25_words", "B1 - 25 most frequent words in FW-15")

    dom = pd.read_csv(f"{d}/top_domains_fw15.csv")
    final_tokens = fun["tokens"].iloc[-1]
    t = dom.copy()
    t.insert(0, "Rank", range(1, len(t) + 1))
    t["share (%)"] = (100 * t["tokens"] / final_tokens).map("{:.3f}".format)
    t["tokens"] = t["tokens"].map("{:,}".format)
    write_table(t, out, "table_4d_top_domains", "B4 - 20 domains contributing the most tokens (final FW-15 corpus)")

    fig, ax = plt.subplots(figsize=(6.5, 4.6))
    ax.barh(dom["domain"], dom["tokens"] / 1e6, 0.7, color=BLUE)
    ax.invert_yaxis()
    ax.set_xlabel("Tokens (millions)")
    ax.grid(axis="y", visible=False)
    ax.set_title(f"Top 20 domains by tokens in the final FW-15 corpus\n(together "
                 f"{100 * dom['tokens'].sum() / final_tokens:.1f}% of all tokens)")
    save(fig, out, "fig4d_top_domains.png")

    # Tokens per Common Crawl snapshot.
    dump = pd.read_csv(f"{d}/tokens_per_dump_fw15.csv").sort_values("dump")
    t = dump.copy()
    t["tokens"] = t["tokens"].map("{:,}".format)
    if "docs" in t:
        t["docs"] = t["docs"].map("{:,}".format)
    write_table(t, out, "table_4d_tokens_per_dump", "B4 - Tokens per Common Crawl snapshot (final FW-15 corpus)")

    fig, ax = plt.subplots(figsize=(11, 3.4))
    x = range(len(dump))
    ax.bar(x, dump["tokens"] / 1e6, 0.8, color=BLUE)
    step = max(1, len(dump) // 24)
    ax.set_xticks(list(x)[::step], [s.replace("CC-MAIN-", "") for s in dump["dump"].iloc[::step]],
                  rotation=60, ha="right", fontsize=7.5)
    ax.set_ylabel("Tokens (millions)")
    ax.set_xlabel("Common Crawl snapshot (CC-MAIN-year-week)")
    ax.grid(axis="x", visible=False)
    ax.set_title(f"Tokens per snapshot in the final FW-15 corpus ({len(dump)} snapshots)")
    save(fig, out, "fig4d_tokens_per_dump.png")


# ======================================================================================
# 4e. Scaling of B2 on cluster B
# ======================================================================================
def part_4e(res, out):
    print("4e: scaling")
    f = f"{res}/part4e/clusterB/scaling_fw2.csv"
    if not os.path.exists(f):
        print("  no scaling csv found, skipped")
        return
    sc = pd.read_csv(f)
    strong = stats(sc[sc["mode"] == "strong"], ["framework", "workers"])
    strong_input = sc[sc["mode"] == "strong"]["input"].iloc[0]
    weak = stats(sc[sc["mode"] == "weak"], ["framework", "workers"])
    weak["source"] = "weak runs"

    # Weak scaling needs N files on N workers. Two points were measured elsewhere under
    # exactly those conditions, so they are reused (and labeled as such in the table):
    #   N=2: input = the first 2 files = the strong-scaling input, with 2 workers;
    #   N=8: input = FW-8, 8 workers = B2 on FW-8 in the pipeline runs (Table 4c).
    extra = []
    if strong_input == "fw2":
        r = strong[strong["workers"] == 2].copy()
        r["source"] = "= strong run, N=2"
        extra.append(r)
    pipe = load_pipeline(res)
    r = pipe[(pipe["stage"] == "B2") & (pipe["dataset"] == "fw8") & (pipe["framework"] != "duckdb")]
    if len(r):
        r = r[["framework", "mean", "lo", "hi", "n"]].copy()
        r["workers"] = 8
        r["source"] = "= Table 4c, B2 on FW-8"
        extra.append(r)
    weak = pd.concat([weak] + extra)
    weak = weak[~weak.duplicated(["framework", "workers"], keep="first")].sort_values(["framework", "workers"])

    def add_metrics(s, kind):
        s = s.copy()
        for fw in s["framework"].unique():
            m = s["framework"] == fw
            t1 = s[m & (s["workers"] == 1)]["mean"]
            if len(t1):
                s.loc[m, "speedup"] = t1.iloc[0] / s.loc[m, "mean"]
                # strong: efficiency = speedup / N ; weak: efficiency = T1 / TN
                s.loc[m, "efficiency"] = s.loc[m, "speedup"] / s.loc[m, "workers"] if kind == "strong" \
                    else s.loc[m, "speedup"]
        return s

    strong = add_metrics(strong, "strong")
    weak = add_metrics(weak, "weak")

    rows = []
    for n in (1, 2, 4, 8):
        row = {"Workers (N)": n}
        for label, s in ((f"Strong, {DS_NAME.get(strong_input, strong_input.upper())}", strong), ("Weak, N files", weak)):
            for fw in ("mapreduce", "spark"):
                r = s[(s["framework"] == fw) & (s["workers"] == n)]
                row[f"{label} {FW_NAME[fw]} (s)"] = fmt_time(r["mean"].iloc[0], r["n"].iloc[0]) if len(r) else "-"
        rows.append(row)
    note = ("Strong scaling uses FW-2 (first 2 files) instead of FW-8: with FW-8 the workers' disks filled up "
            "at N=4 (see part4e/clusterB/fw8_diskfull). Weak-scaling points N=2 and N=8 reuse runs made under the "
            "same conditions: N=2 is the strong-scaling run (2 files, 2 workers), N=8 is B2 on FW-8 from Table 4c.") \
        if strong_input == "fw2" else None
    write_table(pd.DataFrame(rows), out, "table_4e_scaling", "Table 4e - Scaling of B2 on cluster B", note)

    m = []
    for kind, s in (("strong", strong), ("weak", weak)):
        for _, r in s.iterrows():
            m.append({"Scaling": kind, "Framework": FW_NAME[r["framework"]], "N": int(r["workers"]),
                      "Mean (s)": round(r["mean"], 1),
                      "Speedup": round(r["speedup"], 2) if kind == "strong" else "-",
                      "Efficiency": round(r["efficiency"], 2),
                      "Source": r.get("source", "strong runs") if kind == "weak" else "strong runs"})
    write_table(pd.DataFrame(m), out, "table_4e_speedup_efficiency", "Speedup and efficiency (for Q5)",
                "Strong: speedup S(N) = T(1)/T(N), efficiency = S(N)/N. Weak: efficiency = T(1)/T(N).")

    fig, axes = plt.subplots(1, 3, figsize=(11, 3.3))
    ns = [1, 2, 4, 8]
    axes[0].plot(ns, ns, ls=":", color=INK2, lw=1, label="ideal")
    axes[1].axhline(1, ls=":", color=INK2, lw=1)
    axes[2].axhline(1, ls=":", color=INK2, lw=1)
    for fw in ("mapreduce", "spark"):
        st = strong[strong["framework"] == fw].sort_values("workers")
        wk = weak[weak["framework"] == fw].sort_values("workers")
        kw = {"color": FW_COLOR[fw], "marker": "o", "ms": 6, "label": FW_NAME[fw]}
        axes[0].plot(st["workers"], st["speedup"], **kw)
        axes[1].plot(st["workers"], st["efficiency"], **kw)
        axes[2].plot(wk["workers"], wk["efficiency"], **kw)
    sname = DS_NAME.get(strong_input, strong_input.upper())
    for ax, title, yl in ((axes[0], f"Strong scaling ({sname}): speedup", "Speedup T(1)/T(N)"),
                          (axes[1], f"Strong scaling ({sname}): efficiency", "Efficiency S(N)/N"),
                          (axes[2], "Weak scaling (N files): efficiency", "Efficiency T(1)/T(N)")):
        ax.set_xscale("log", base=2)
        ax.set_xticks(ns, [str(n) for n in ns])
        ax.minorticks_off()
        ax.set_xlabel("Workers (N)")
        ax.set_ylabel(yl)
        ax.set_title(title)
        ax.set_ylim(bottom=0)
    axes[0].legend(loc="upper left")
    save(fig, out, "fig4e_scaling.png")


# ======================================================================================
# Tables for the questions (Q1, Q3, Q8, Q9)
# ======================================================================================
def mr_counters(log):
    """Read the counters MapReduce prints at the end of `hadoop jar` (name=value lines)."""
    c = {}
    with open(log, errors="replace") as f:
        for line in f:
            m = re.match(r"^\s+([A-Za-z][^=]*?)=(\d+)\s*$", line)
            if m:
                c[m.group(1).strip()] = int(m.group(2))
    return c


def gb(x):
    return f"{x / 1e9:.2f}"


def part_questions(res, out):
    print("questions: Q1, Q3, Q8, Q9")
    # Q1: bytes moved by the MapReduce sorts on 64GB (Spark: read them in the Spark UI / event logs).
    rows = []
    for log in sorted(glob.glob(f"{res}/part2/cluster*/*sort_64GB_r1.log")):
        sort = os.path.basename(log).split("_")[0]
        if sort == "sparksort":
            continue
        c = mr_counters(log)
        rows.append({"Cluster": log.split("/")[-2][-1], "Sort": SORT_NAME[sort],
                     "HDFS read (GB)": gb(c.get("HDFS: Number of bytes read", 0)),
                     "HDFS written (GB)": gb(c.get("HDFS: Number of bytes written", 0)),
                     "Local disk read (GB)": gb(c.get("FILE: Number of bytes read", 0)),
                     "Local disk written (GB)": gb(c.get("FILE: Number of bytes written", 0)),
                     "Shuffle (GB)": gb(c.get("Reduce shuffle bytes", 0)),
                     "Spilled records / map output records":
                         f"{c.get('Spilled Records', 0) / max(1, c.get('Map output records', 1)):.2f}"})
    if rows:
        write_table(pd.DataFrame(rows), out, "q1_mr_counters", "Q1 - MapReduce counters of the 64GB sorts",
                    "From the counters printed at the end of each job (1 GB = 10^9 bytes).")

    pipe = load_pipeline(res)

    def pmean(stage, fw, ds):
        r = pipe[(pipe["stage"] == stage) & (pipe["framework"] == fw) & (pipe["dataset"] == ds)]
        return r["mean"].iloc[0] if len(r) else float("nan")

    # Q3: combiner on/off (MapReduce) and reduceByKey vs groupByKey (Spark), FW-1.
    q3 = f"{res}/questions/q3/q3.csv"
    if os.path.exists(q3):
        q = pd.read_csv(q3, header=None, names=["run", "seconds"])
        q["variant"] = q["run"].str.rsplit("_r", n=1).str[0]
        qm = q.groupby("variant")["seconds"].agg(["mean", "count"])
        with_c = [mr_counters(f) for f in sorted(glob.glob(f"{res}/part3/clusterB/B1_mapreduce_fw1_r*.log"))]
        without = [mr_counters(f) for f in sorted(glob.glob(f"{res}/questions/q3/mr_nocombiner_r*.log"))]
        rows = []
        for name, cs, t in (("MapReduce, with combiner", with_c, pmean("B1", "mapreduce", "fw1")),
                            ("MapReduce, without combiner", without, qm.loc["mr_nocombiner", "mean"]
                             if "mr_nocombiner" in qm.index else float("nan"))):
            c = cs[0] if cs else {}
            rows.append({"Variant": name, "Time (s)": f"{t:.1f}",
                         "Map output records": f"{c.get('Map output records', 0):,}",
                         "Combine output records": f"{c.get('Combine output records', 0):,}",
                         "Reduce shuffle bytes": f"{c.get('Reduce shuffle bytes', 0):,}"})
        rows.append({"Variant": "Spark, reduceByKey", "Time (s)": f"{pmean('B1', 'spark', 'fw1'):.1f}",
                     "Map output records": "-", "Combine output records": "-", "Reduce shuffle bytes": "see spark_shuffle.txt"})
        if "spark_groupbykey" in qm.index:
            rows.append({"Variant": "Spark, groupByKey (1 run)", "Time (s)": f"{qm.loc['spark_groupbykey', 'mean']:.1f}",
                         "Map output records": "-", "Combine output records": "-",
                         "Reduce shuffle bytes": "see spark_shuffle_groupbykey.txt"})
        write_table(pd.DataFrame(rows), out, "q3_combiner", "Q3 - Word count on FW-1, with and without a combiner",
                    "Times are means of 3 runs unless marked; counters are from the first run of each variant.")

    # Q8: TSV vs Parquet input on FW-8.
    q8 = f"{res}/questions/q8/q8.csv"
    if os.path.exists(q8):
        q = pd.read_csv(q8, header=None, names=["run", "seconds"])
        q["stage"] = q["run"].str.split("_").str[0].str.upper()
        qm = q.groupby("stage")["seconds"].mean()
        inp = {}
        ib = f"{res}/questions/q8/input_bytes.txt"
        if os.path.exists(ib):
            for line in open(ib):
                m = re.search(r"(B\d)_\w*?(parquet|spark_fw8)\S*\s+input=([\d.]+ \w+)", line, re.I)
                if m:
                    inp[(m.group(1).upper(), "Parquet" if "parquet" in m.group(2).lower() else "TSV")] = m.group(3)
        rows = []
        for st in ("B1", "B4"):
            tsv, par = pmean(st, "spark", "fw8"), qm.get(st, float("nan"))
            rows.append({"Stage": st, "TSV time (s)": f"{tsv:.1f}", "Parquet time (s)": f"{par:.1f}",
                         "Gain": f"{100 * (1 - par / tsv):.0f}%",
                         "TSV input": inp.get((st, "TSV"), "-"), "Parquet input": inp.get((st, "Parquet"), "-")})
        write_table(pd.DataFrame(rows), out, "q8_parquet", "Q8 - Spark on FW-8, reading TSV vs Parquet",
                    "Means of 3 runs. Input = bytes read, from the Spark UI (input_bytes.txt).")

    # Q9: losing a NodeManager halfway through B2 on FW-8.
    q9 = f"{res}/questions/q9/q9.csv"
    if os.path.exists(q9):
        q = pd.read_csv(q9)
        q["Framework"] = q["framework"].map(FW_NAME)
        q["Extra time (s)"] = (q["seconds"] - q["normal_mean"]).round(1)
        q["Extra (%)"] = (100 * (q["seconds"] / q["normal_mean"] - 1)).round(0)
        t = q[["Framework", "normal_mean", "seconds", "Extra time (s)", "Extra (%)", "victim", "output_ok"]]
        t.columns = ["Framework", "Normal (s)", "With failure (s)", "Extra time (s)", "Extra (%)",
                     "Stopped NodeManager", "Output identical"]
        write_table(t, out, "q9_failure", "Q9 - B2 on FW-8 with one NodeManager stopped halfway")


# ======================================================================================
def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--results", default="results", help="results directory (default: results)")
    ap.add_argument("--out", default="report", help="output directory (default: report)")
    ap.add_argument("--only", nargs="*", help="run only some parts, e.g. --only 4a 4e")
    a = ap.parse_args()
    for sub in ("figures", "tables"):
        os.makedirs(os.path.join(a.out, sub), exist_ok=True)
    setup_style()
    parts = {"4a": part_4a, "4b": part_4b, "4c": part_4c, "4d": part_4d, "4e": part_4e, "q": part_questions}
    for key, fn in parts.items():
        if a.only and key not in a.only:
            continue
        try:
            fn(a.results, a.out)
        except FileNotFoundError as e:
            print(f"  {key}: missing input ({e.filename}), skipped")


if __name__ == "__main__":
    main()
