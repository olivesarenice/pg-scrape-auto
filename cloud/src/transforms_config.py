from dataclasses import dataclass
from typing import List, Optional

import transforms


@dataclass
class TransformStep:
    transform_fn: callable
    input_cols: list
    output_cols: list


@dataclass
class TransformConfig:
    transformations: List[
        TransformStep
    ]  # This is a list of of steps dictating transformation functions to be applied to the intermediate table
    dest_table_name: str  # This is the name of the destination table (saved in s3)
    dest_parquet: dict
    dest_bq: dict
    primary_key: Optional[list] = None


listings_config = TransformConfig(
    transformations=[
        TransformStep(
            transform_fn=transforms.add_ts,
            input_cols=[],
            output_cols=["transform_ts"],
        ),
        TransformStep(
            transform_fn=transforms.clean_floorarea,
            input_cols=["floor_area"],
            output_cols=["floor_area"],
        ),
        TransformStep(
            transform_fn=transforms.is_new_project,
            input_cols=["is_new_project"],
            output_cols=["is_new_project"],
        ),
        TransformStep(
            transform_fn=transforms.is_turbo,
            input_cols=["is_turbo_listing"],
            output_cols=["is_turbo_listing"],
        ),
        TransformStep(
            transform_fn=transforms.form_property_type,
            input_cols=["category", "search_type"],
            output_cols=["property_segment", "property_type"],
        ),
        TransformStep(
            transform_fn=transforms.clean_headline,
            input_cols=["headline"],
            output_cols=["headline"],
        ),
        TransformStep(
            transform_fn=transforms.parse_proximity,
            input_cols=["proximity_mrt"],
            output_cols=["mrt_nearest", "mrt_walk_time_min", "mrt_distance_m"],
        ),
        TransformStep(
            transform_fn=transforms.calculate_post_time,
            input_cols=["recency", "partition_ts"],
            output_cols=["listing_created_after"],
        ),
        TransformStep(
            transform_fn=transforms.remove_bad_data,
            input_cols=["district_code", "price", "floor_area"],
            output_cols=[],
        ),
        TransformStep(
            transform_fn=transforms.get_psf,
            input_cols=["price", "floor_area"],
            output_cols=["psf"],
        ),
    ],
    dest_table_name="listings",
    dest_parquet={
        "id": "int_string",
        "transform_ts": "datetime64[ms]",
        "partition_ts": "datetime64[ms]",
        "name": "string",
        "property_segment": "string",
        "property_type": "string",
        "project": "int_string",
        "is_new_project": "int_string",
        "developer": "string",
        "price": "Int32",
        "floor_area": "Int32",
        "psf": "Int32",
        "bedrooms": "Int32",
        "bathrooms": "Int32",
        "is_turbo_listing": "int_string",
        "district": "string",
        "district_code": "string",
        "region": "string",
        "region_code": "string",
        "search_type": "string",
        "url": "string",
        "title": "string",
        "address": "string",
        "agent_id": "string",
        "agent_name": "string",
        "headline": "string",
        "mrt_nearest": "string",
        "mrt_walk_time_min": "Int32",
        "mrt_distance_m": "Int32",
        "listing_created_after": "datetime64[ms]",
    },
    dest_bq={
        "id": "STRING",
        "transform_ts": "TIMESTAMP",
        "partition_ts": "TIMESTAMP",
        "name": "STRING",
        "property_segment": "STRING",
        "property_type": "STRING",
        "project": "STRING",
        "is_new_project": "STRING",
        "developer": "STRING",
        "price": "INTEGER",
        "floor_area": "INTEGER",
        "psf": "INTEGER",
        "bedrooms": "INTEGER",
        "bathrooms": "INTEGER",
        "is_turbo_listing": "STRING",
        "district": "STRING",
        "district_code": "STRING",
        "region": "STRING",
        "region_code": "STRING",
        "search_type": "STRING",
        "url": "STRING",
        "title": "STRING",
        "address": "STRING",
        "agent_id": "STRING",
        "agent_name": "STRING",
        "headline": "STRING",
        "mrt_nearest": "STRING",
        "mrt_walk_time_min": "INTEGER",
        "mrt_distance_m": "INTEGER",
        "listing_created_after": "TIMESTAMP",
    },
    primary_key=["id", "partition_ts"],
)
