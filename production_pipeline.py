#!/usr/bin/env python3
import argparse
import pipeline
import pipeline_runner  # installs safe feature/model functions
from relevance import classify_theme, score_relevance

pipeline.classify_theme = classify_theme
pipeline.score_relevance = score_relevance

if __name__ == '__main__':
    p=argparse.ArgumentParser(); p.add_argument('job',choices=['markets','models','all']); args=p.parse_args()
    pipeline.run(args.job)
