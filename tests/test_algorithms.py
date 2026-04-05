import numpy as np
import pandas as pd
import pytest

from tests import df1, df2
from valentine.algorithms import (
    Coma,
    Cupid,
    DistributionBased,
    JaccardDistanceMatcher,
    SimilarityFlooding,
)
from valentine.algorithms.jaccard_distance import StringDistanceFunction
from valentine.data_sources import DataframeTable

d1 = DataframeTable(df1, name="source")
d2 = DataframeTable(df2, name="target")


def test_coma():
    # Test the schema variant of Coma
    coma_schema = Coma(use_instances=False)
    matches_schema = coma_schema.get_matches(d1, d2)
    assert len(matches_schema) > 0
    # Test the instance variant of Coma
    coma_instances = Coma(use_instances=True)
    matches_instances = coma_instances.get_matches(d1, d2)
    assert len(matches_instances) > 0
    # Schema and instance should provide different results
    assert matches_schema != matches_instances


def test_cupid():
    # Test the CUPID matcher
    cu_matcher = Cupid()
    matches_cu_matcher = cu_matcher.get_matches(d1, d2)
    # Check that it actually produced output
    assert len(matches_cu_matcher) > 0
    cu_matcher = Cupid(process_num=2)
    matches_cu_matcher = cu_matcher.get_matches(d1, d2)
    # Check that it actually produced output
    assert len(matches_cu_matcher) > 0


def test_cupid_paper_figure7():
    """Test based on Figure 7 from the Cupid paper (Madhavan et al. 2001).

    Two purchase order schemas (CIDX and Excel) with columns that have similar
    but not identical names. Verifies that Cupid's linguistic + structural
    matching correctly identifies correspondences across naming variations.
    """
    rng = np.random.default_rng(42)
    n = 5

    # CIDX Purchase Order schema (leaf elements from Figure 7)
    cidx_df = pd.DataFrame(
        {
            "PONumber": rng.integers(1000, 9999, n),
            "PODate": pd.date_range("2024-01-01", periods=n),
            "ContactName": [f"Name{i}" for i in range(n)],
            "ContactEmail": [f"email{i}@example.com" for i in range(n)],
            "ContactPhone": [f"555-000{i}" for i in range(n)],
            "Street1": [f"{i} Main St" for i in range(n)],
            "City": [f"City{i}" for i in range(n)],
            "StateProvince": [f"State{i}" for i in range(n)],
            "PostalCode": rng.integers(10000, 99999, n),
            "Country": [f"Country{i}" for i in range(n)],
            "unitPrice": rng.random(n) * 100,
            "qty": rng.integers(1, 100, n),
            "partno": rng.integers(100, 999, n),
        }
    )

    # Excel Purchase Order schema (leaf elements from Figure 7)
    excel_df = pd.DataFrame(
        {
            "orderNum": rng.integers(1000, 9999, n),
            "orderDate": pd.date_range("2024-01-01", periods=n),
            "contactName": [f"Name{i}" for i in range(n)],
            "companyName": [f"Company{i}" for i in range(n)],
            "telephone": [f"555-000{i}" for i in range(n)],
            "street1": [f"{i} Main St" for i in range(n)],
            "city": [f"City{i}" for i in range(n)],
            "stateProvince": [f"State{i}" for i in range(n)],
            "postalCode": rng.integers(10000, 99999, n),
            "country": [f"Country{i}" for i in range(n)],
            "unitPrice": rng.random(n) * 100,
            "Quantity": rng.integers(1, 100, n),
            "partNumber": rng.integers(100, 999, n),
        }
    )

    cidx_table = DataframeTable(cidx_df, name="CIDX_PO")
    excel_table = DataframeTable(excel_df, name="Excel_PO")

    matcher = Cupid()
    matches = matcher.get_matches(cidx_table, excel_table)

    assert len(matches) > 0

    # Extract matched column name pairs for easier assertion
    matched_pairs = {}
    for pair, score in matches.items():
        matched_pairs[(pair.source_column, pair.target_column)] = score

    # Columns with identical or near-identical names should be matched
    # (case-insensitive matches that Cupid's linguistic matching handles)
    expected_matches = [
        ("unitPrice", "unitPrice"),
        ("City", "city"),
        ("StateProvince", "stateProvince"),
        ("PostalCode", "postalCode"),
        ("Country", "country"),
        ("ContactName", "contactName"),
        ("Street1", "street1"),
    ]

    for col1, col2 in expected_matches:
        assert (col1, col2) in matched_pairs or (col2, col1) in matched_pairs, (
            f"Expected match ({col1}, {col2}) not found in results. "
            f"Matched pairs: {list(matched_pairs.keys())}"
        )


def _cupid_matched_pairs(source_table, target_table):
    """Run Cupid and return {(col1, col2): score} dict."""
    matcher = Cupid()
    matches = matcher.get_matches(source_table, target_table)
    pairs = {}
    for pair, score in matches.items():
        pairs[(pair.source_column, pair.target_column)] = score
    return pairs


def _assert_match_found(matched_pairs, col1, col2):
    assert (col1, col2) in matched_pairs or (col2, col1) in matched_pairs, (
        f"Expected match ({col1}, {col2}) not found. Matched pairs: {list(matched_pairs.keys())}"
    )


def test_cupid_paper_figure8_products():
    """Figure 8: RDB Products vs Star Products.

    Both schemas have a Products table with nearly identical columns:
    ProductID, ProductName, BrandID, BrandDescription.
    """
    rng = np.random.default_rng(42)
    n = 5

    # RDB Schema PRODUCTS table
    rdb_products = pd.DataFrame(
        {
            "ProductID": rng.integers(1, 1000, n),
            "BrandID": rng.integers(1, 50, n),
            "ProductName": [f"Product{i}" for i in range(n)],
            "BrandDescription": [f"Brand{i}" for i in range(n)],
        }
    )

    # Star Schema PRODUCTS table
    star_products = pd.DataFrame(
        {
            "ProductID": rng.integers(1, 1000, n),
            "ProductName": [f"Product{i}" for i in range(n)],
            "BrandID": rng.integers(1, 50, n),
            "BrandDescription": [f"Brand{i}" for i in range(n)],
        }
    )

    matched = _cupid_matched_pairs(
        DataframeTable(rdb_products, name="RDB_Products"),
        DataframeTable(star_products, name="Star_Products"),
    )

    assert len(matched) > 0
    for col in ["ProductID", "ProductName", "BrandID", "BrandDescription"]:
        _assert_match_found(matched, col, col)


def test_cupid_paper_figure8_customers():
    """Figure 8: RDB Customers vs Star Customers.

    The RDB table uses CompanyName, ContactFirstName, ContactLastName,
    StateOrProvince while the Star table uses CustomerName, PostalCode,
    CustomerTypeID, CustomerTypeDescription, State. Cupid should match
    columns with identical names (CustomerID, PostalCode) and similar
    names (StateOrProvince vs State).
    """
    rng = np.random.default_rng(42)
    n = 5

    # RDB Schema CUSTOMERS table
    rdb_customers = pd.DataFrame(
        {
            "CustomerID": rng.integers(1, 1000, n),
            "CompanyName": [f"Company{i}" for i in range(n)],
            "ContactFirstName": [f"First{i}" for i in range(n)],
            "ContactLastName": [f"Last{i}" for i in range(n)],
            "BillingAddress": [f"{i} Elm St" for i in range(n)],
            "City": [f"City{i}" for i in range(n)],
            "StateOrProvince": [f"State{i}" for i in range(n)],
            "PostalCode": rng.integers(10000, 99999, n),
            "Country": [f"Country{i}" for i in range(n)],
            "PhoneNumber": [f"555-{i:04d}" for i in range(n)],
        }
    )

    # Star Schema CUSTOMERS table
    star_customers = pd.DataFrame(
        {
            "CustomerID": rng.integers(1, 1000, n),
            "CustomerName": [f"Customer{i}" for i in range(n)],
            "CustomerTypeID": rng.integers(1, 10, n),
            "CustomerTypeDescription": [f"Type{i}" for i in range(n)],
            "PostalCode": rng.integers(10000, 99999, n),
            "State": [f"State{i}" for i in range(n)],
        }
    )

    matched = _cupid_matched_pairs(
        DataframeTable(rdb_customers, name="RDB_Customers"),
        DataframeTable(star_customers, name="Star_Customers"),
    )

    assert len(matched) > 0
    _assert_match_found(matched, "CustomerID", "CustomerID")
    _assert_match_found(matched, "PostalCode", "PostalCode")


def test_cupid_paper_figure8_orders_sales():
    """Figure 8: RDB Orders/OrderDetails vs Star Sales.

    The paper notes Cupid matches the join of Orders and OrderDetails to Sales.
    Since Valentine handles flat tables, we represent the RDB side as a
    denormalized Orders table and match against the Star Sales table.
    Shared columns: OrderID, OrderDetailID, CustomerID, ProductID, OrderDate,
    Quantity, UnitPrice, Discount.
    """
    rng = np.random.default_rng(42)
    n = 5

    # RDB Schema: denormalized ORDERS + ORDERDETAILS
    rdb_orders = pd.DataFrame(
        {
            "OrderID": rng.integers(1, 10000, n),
            "OrderDetailID": rng.integers(1, 50000, n),
            "CustomerID": rng.integers(1, 1000, n),
            "ProductID": rng.integers(1, 500, n),
            "OrderDate": pd.date_range("2024-01-01", periods=n),
            "Quantity": rng.integers(1, 100, n),
            "UnitPrice": rng.random(n) * 100,
            "Discount": rng.random(n) * 0.5,
            "ShipName": [f"Ship{i}" for i in range(n)],
            "ShipAddress": [f"{i} Oak Ave" for i in range(n)],
            "FreightCharge": rng.random(n) * 50,
        }
    )

    # Star Schema: SALES fact table
    star_sales = pd.DataFrame(
        {
            "OrderID": rng.integers(1, 10000, n),
            "OrderDetailID": rng.integers(1, 50000, n),
            "CustomerID": rng.integers(1, 1000, n),
            "PostalCode": rng.integers(10000, 99999, n),
            "ProductID": rng.integers(1, 500, n),
            "OrderDate": pd.date_range("2024-01-01", periods=n),
            "Quantity": rng.integers(1, 100, n),
            "UnitPrice": rng.random(n) * 100,
            "Discount": rng.random(n) * 0.5,
        }
    )

    matched = _cupid_matched_pairs(
        DataframeTable(rdb_orders, name="RDB_Orders"),
        DataframeTable(star_sales, name="Star_Sales"),
    )

    assert len(matched) > 0
    for col in ["OrderID", "OrderDetailID", "CustomerID", "ProductID", "Quantity", "UnitPrice"]:
        _assert_match_found(matched, col, col)


def test_cupid_paper_figure8_geography():
    """Figure 8: RDB Territories/Region vs Star Geography.

    The paper notes the Geography table columns map to Region and Territory
    columns. Shared concepts: TerritoryID, TerritoryDescription, RegionID,
    RegionDescription, PostalCode.
    """
    rng = np.random.default_rng(42)
    n = 5

    # RDB Schema: denormalized TERRITORIES + REGION
    rdb_territories = pd.DataFrame(
        {
            "TerritoryID": rng.integers(1, 100, n),
            "TerritoryDescription": [f"Territory{i}" for i in range(n)],
            "RegionID": rng.integers(1, 10, n),
            "RegionDescription": [f"Region{i}" for i in range(n)],
        }
    )

    # Star Schema: GEOGRAPHY dimension table
    star_geography = pd.DataFrame(
        {
            "PostalCode": rng.integers(10000, 99999, n),
            "CustomerName": [f"Customer{i}" for i in range(n)],
            "TerritoryDescription": [f"Territory{i}" for i in range(n)],
            "TerritoryID": rng.integers(1, 100, n),
            "RegionID": rng.integers(1, 10, n),
            "RegionDescription": [f"Region{i}" for i in range(n)],
        }
    )

    matched = _cupid_matched_pairs(
        DataframeTable(rdb_territories, name="RDB_Territories"),
        DataframeTable(star_geography, name="Star_Geography"),
    )

    assert len(matched) > 0
    for col in ["TerritoryID", "TerritoryDescription", "RegionID", "RegionDescription"]:
        _assert_match_found(matched, col, col)


def test_distribution_based():
    # Test the Distribution based matcher
    distribution_based_matcher = DistributionBased()
    matches_db_matcher = distribution_based_matcher.get_matches(d1, d2)
    # Check that it actually produced output
    assert len(matches_db_matcher) > 0
    distribution_based_matcher = DistributionBased(process_num=2)
    matches_db_matcher = distribution_based_matcher.get_matches(d1, d2)
    # Check that it actually produced output
    assert len(matches_db_matcher) > 0


def test_jaccard():
    # Test the Jaccard matcher with exact string similarity
    jd_matcher = JaccardDistanceMatcher(distance_fun=StringDistanceFunction.Exact)
    matches_jd_matcher = jd_matcher.get_matches(d1, d2)
    # Check that it actually produced output
    assert len(matches_jd_matcher) > 0


@pytest.mark.parametrize(
    "distance_function",
    [
        StringDistanceFunction.Hamming,
        StringDistanceFunction.Levenshtein,
        StringDistanceFunction.DamerauLevenshtein,
        StringDistanceFunction.JaroWinkler,
        StringDistanceFunction.Jaro,
    ],
)
def test_jaccard_distance_function(distance_function):
    # Test the Jaccard matcher with different distance functions
    jd_matcher = JaccardDistanceMatcher(distance_fun=distance_function)
    matches_jd_matcher = jd_matcher.get_matches(d1, d2)
    # Check that it actually produced output
    assert len(matches_jd_matcher) > 0
    jd_matcher = JaccardDistanceMatcher(
        threshold_dist=0.5, process_num=2, distance_fun=distance_function
    )
    matches_jd_matcher = jd_matcher.get_matches(d1, d2)
    # Check that it actually produced output
    assert len(matches_jd_matcher) > 0


def test_similarity_flooding():
    # Test the Similarity flooding matcher
    sf_matcher = SimilarityFlooding()
    matches_sf_matcher = sf_matcher.get_matches(d1, d2)
    # Check that it actually produced output
    assert len(matches_sf_matcher) > 0
