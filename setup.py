#!/usr/bin/env python3
"""Setup for the DuckDB-first local BoatRace prediction tooling."""

from pathlib import Path
from setuptools import find_namespace_packages, find_packages, setup

# READMEの読み込み
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text(encoding="utf-8")

# 依存関係の読み込み
requirements = []
requirements_file = this_directory / "requirements.txt"
if requirements_file.exists():
    with open(requirements_file) as f:
        requirements = [line.strip() for line in f if line.strip() and not line.startswith("#")]

setup(
    name="boatrace-prediction-system",
    version="1.0.0",
    author="BoatRace Team",
    author_email="admin@boatrace-predict.com",
    description="ボートレース予測システム - DuckDB ローカル予測パイプライン",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/miyamamoto/boatracedb-public",
    license="Apache-2.0",
    packages=find_packages(where="src") + find_namespace_packages(where=".", include=["scripts", "scripts.*"]),
    package_dir={"": "src", "scripts": "scripts"},
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Build Tools",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.11",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.11",
    install_requires=requirements + [
        "click>=8.0.0",
        "tabulate>=0.9.0",
        "pyyaml>=6.0",
    ],
    entry_points={
        "console_scripts": [
            "boatrace-local-pipeline=scripts.boatrace_local_pipeline:main",
            "boatrace-prediction-query=scripts.boatrace_prediction_query:main",
            "boatrace-bootstrap=scripts.boatrace_bootstrap:main",
            "boatrace-program-sheet=scripts.boatrace_program_sheet:main",
            "boatrace-agentic-test=scripts.run_agentic_chaos_tests:main",
            "boatrace-agentic-reference-sim=scripts.run_agentic_reference_simulation:main",
            "boatrace-agentic-recorder=scripts.run_agentic_transcript_recorder:main",
        ],
    },
    include_package_data=True,
    package_data={
        "": ["*.yaml", "*.yml", "*.json"],
    },
    project_urls={
        "Bug Reports": "https://github.com/miyamamoto/boatracedb-public/issues",
        "Source": "https://github.com/miyamamoto/boatracedb-public",
    },
)
