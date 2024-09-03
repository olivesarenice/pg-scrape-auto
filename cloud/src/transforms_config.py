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
    dest_parquet: (
        dict  # These are the columns to keep in the destination table (saved in s3)
    )
    primary_key: Optional[list] = None
    RDS_varchar: Optional[dict] = (
        None  # only for varchar columns. Trim the string to this length
    )


listings_config = TransformConfig(
    transformations=[
        TransformStep(
            transform_fn=transforms.add_ts,
            input_cols=[],
            output_cols=["partition_ts"],
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
        "id": "Int32",
        "partition_ts": "datetime64[ns]",
        "name": "string",
        "property_segment": "string",
        "property_type": "string",
        "project": "string",
        "is_new_project": "string",
        "developer": "string",
        "price": "Int32",
        "floor_area": "Int32",
        "psf": "Int32",
        "bedrooms": "string",
        "bathrooms": "string",
        "is_turbo_listing": "string",
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
        "listing_created_after": "datetime64[ns]",
    },
    primary_key=["id, device_name_unnest"],
)
