#!/usr/bin/env python
"""
Performs basic cleaning on the data and save the results in Weights & Biases
"""
import argparse
import logging
import wandb


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
    artifact_local_path = run.use_artifact(args.input_artifact).file()
    df = pd.read_csv(artifact_local_path)
    
    # drop outliers
    logger.info(f'Drop outliers regarding min {args.min_price}, max {args.max_price} price thresholds')
    min_price = args.min_price
    max_price = args.max_price
    idx = df['price'].between(min_price, max_price)
    df_clean = df[idx].copy()
    
    # normal distribution of 'minimum_nights'
    logger.info('Transform skewness of feature "minimum_nights" to normal distribution')
    df_clean['minimum_nights'] = np.log(df_clean['minimum_nights'])
    
    # convert 'last_review' to datetime
    logger.info('Convert feature "last_review" to datetime type')
    df_clean['last_review'] = pd.to_datetime(df_clean['last_review'])
    
    # drop rows in the dataset that are not in the proper geolocation
    logger.info('Drop rows in the dataset that are not in the proper geolocation')
    idx = df_clean['longitude'].between(-74.25, -73.50) & df_clean['latitude'].between(40.5, 41.2)
    df_clean = df_clean[idx].copy()
    
    # save cleaned dataframe
    logger.info(f'Save cleaned dataframe to {args.output_artifact_name}')
    df_clean.to_csv(args.output_artifact_name, index=False)

    # log artifact to Weights & Biases
    logger.info(f'W&B logging artifact {args.output_artifact_name}') 
    artifact = wandb.Artifact(
        name=args.output_artifact_name,
        type=args.output_artifact_type,
        description=args.output_artifact_description,
    )
    artifact.add_file(args.output_artifact_name)
    run.log_artifact(artifact)
    

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="This steps cleans the data")


    parser.add_argument(
        "--parameter1", 
        type=## INSERT TYPE HERE: str, float or int,
        help=## INSERT DESCRIPTION HERE,
        required=True
    )

    parser.add_argument(
        "--parameter2", 
        type=## INSERT TYPE HERE: str, float or int,
        help=## INSERT DESCRIPTION HERE,
        required=True
    )

    parser.add_argument(
        "--parameter3", 
        type=## INSERT TYPE HERE: str, float or int,
        help=## INSERT DESCRIPTION HERE,
        required=True
    )


    args = parser.parse_args()

    go(args)
