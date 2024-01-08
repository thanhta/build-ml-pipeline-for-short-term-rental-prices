#!/usr/bin/env python
"""
Performs basic cleaning on the data and save the results in Weights & Biases
"""
import argparse
import logging
import wandb
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    ######################
    # YOUR CODE HERE     #
    ######################
    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    # artifact_local_path = run.use_artifact(args.input_artifact).file()

    logger.info(f'Download input artifact {args.input_artifact}')
    artifact_path = run.use_artifact(args.input_artifact).file()
    df = pd.read_csv(artifact_path)
    
    # drop outliers
    logger.info(f'Drop outliers and keep price within min price: {args.min_price} to max price {args.max_price}')
    min_price = args.min_price
    max_price = args.max_price
    idx = df['price'].between(min_price, max_price)
    df = df[idx].copy()
    
    # convert 'last_review' to datetime
    logger.info('Convert "last_review" column to datetime type')
    df['last_review'] = pd.to_datetime(df['last_review'])
    
    # Drop rows in the dataset that are not in the proper geolocation.
    logger.info('Drop rows in the dataset that are not in the proper geolocation')
    idx = df['longitude'].between(-74.25, -73.50) & df['latitude'].between(40.5, 41.2)
    df = df[idx].copy()
    
    # Dropping Duplicates
    logger.info('Dropping duplicate rows')
    df = df.drop_duplicates().reset_index(drop=True)
    
    # save cleaned artifact
    logger.info(f'Save cleaned artifact to {args.output_artifact_name}')
    df.to_csv(args.output_artifact_name, index=False)

    # log artifact to Weights & Biases
    logger.info(f'Logging artifact {args.output_artifact_name} to W&B') 
    artifact = wandb.Artifact(
        name=args.output_artifact_name,
        type=args.output_artifact_type,
        description=args.output_artifact_description,
    )
    artifact.add_file(args.output_artifact_name)

    logger.info('Logging artifact')
    run.log_artifact(artifact)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="This steps handles the data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="Name of the input artifact",
        required=True
    )

    parser.add_argument(
        "--output_artifact_name", 
        type=str,
        help="Name of the output artifact",
        required=True
    )

    parser.add_argument(
        "--output_artifact_type", 
        type=str,
        help="Data type of the output artifact",
        required=True
    )

    parser.add_argument(    
        "--output_artifact_description", 
        type=str,
        help="Description of the output artifact",
        required=True
    )

    parser.add_argument(    
        "--min_price", 
        type=float,
        help="Minimum price value to be considered",
        required=True
    )

    parser.add_argument(    
        "--max_price", 
        type=float,
        help="Maximum price value to be considered",
        required=True
    )


    args = parser.parse_args()

    go(args)
