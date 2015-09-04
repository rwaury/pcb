package de.tuberlin.dima.ti.analysis;

/* midt only,
         * db1b only, db1b(us only)/midt(rest)
         * midt/db1b merged
         * midt/db1b merged -> average of two models vs. combined model
         * db1b -> spread vs. replicated
         * merge -> Euclidean vs. Mahalanobis
         * Logit -> MNL,C-Logit, PSL
         */
public enum TrainingData {
    MIDT,
    DB1B,
    BOTH_MERGED,
    BOTH_US_SEPARATE
}
