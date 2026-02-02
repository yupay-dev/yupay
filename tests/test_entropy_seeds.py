import pytest
import polars as pl
from yupay.core.entropy import EntropyManager


def test_entropy_determinism():
    """
    Verify that identical seeds produce identical results.
    """
    config_a = {
        "chaos_level": "medium",  # Ensure some probability
        "chaos": {
            "global_seed": 42,
            "levels": {"medium": {"null_probability": 0.5}}
        }
    }

    config_b = {
        "chaos_level": "medium",
        "chaos": {
            "global_seed": 42,  # SAME SEED
            "levels": {"medium": {"null_probability": 0.5}}
        }
    }

    # DataFrame
    df = pl.DataFrame({"val": range(100)})

    # Run A
    mgr_a = EntropyManager(config_a)
    res_a = mgr_a.inject_nulls(df.lazy(), ["val"]).collect()

    # Run B
    mgr_b = EntropyManager(config_b)
    res_b = mgr_b.inject_nulls(df.lazy(), ["val"]).collect()

    # Assert Equality
    assert res_a.equals(res_b), "Same seed should produce identical results"


def test_entropy_variance():
    """
    Verify that different seeds produce different results.
    """
    config_a = {
        "chaos_level": "medium",
        "chaos": {
            "global_seed": 42,
            "levels": {"medium": {"null_probability": 0.5}}
        }
    }

    config_c = {
        "chaos_level": "medium",
        "chaos": {
            "global_seed": 99999,  # DIFFERENT SEED
            "levels": {"medium": {"null_probability": 0.5}}
        }
    }

    # Larger sample to avoid lucky match
    df = pl.DataFrame({"val": range(1000)})

    mgr_a = EntropyManager(config_a)
    res_a = mgr_a.inject_nulls(df.lazy(), ["val"]).collect()

    mgr_c = EntropyManager(config_c)
    res_c = mgr_c.inject_nulls(df.lazy(), ["val"]).collect()

    # Assert Inequality (Probability of collision 1000 rows 50% null is astronomical)
    assert not res_a.equals(
        res_c), "Different seeds should produce different results"
