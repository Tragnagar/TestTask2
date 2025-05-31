from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def get_product_category_pairs(products_df, categories_df, relations_df):
    # ��������� �������� � ����� (����� LEFT JOIN)
    products_with_relations = products_df.join(relations_df, products_df["product_id"] == relations_df["product_id"], "left")

    # ��������� ��������� � ����������� (����� LEFT JOIN)
    result = products_with_relations.join(categories_df, products_with_relations["category_id"] == categories_df["category_id"], "left").select(
        products_df["product_name"], categories_df["category_name"])

    # �������� �������� �������� ��� ���������
    products_without_categories = products_df.join(
        relations_df, products_df["product_id"] == relations_df["product_id"], "left-anti").select(products_df["product_name"])

    return result, products_without_categories