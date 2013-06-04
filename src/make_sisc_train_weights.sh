#!/bin/bash
    for r in $(seq 1 16); do
    python compute_interp_weights.py model/train-svd/Vt.txt.out \
      model/train-svd/Sigma.txt.out \
      sisc-trainset-design.txt  sisc-testset-design.txt R $r > sisc-train-weights-$r.txt
    done
