# -*- coding: utf-8 -*-
import click
import logging
from pathlib import Path
from dotenv import find_dotenv, load_dotenv
import os
import pandas as pd
import fsspec
from beer_web_scraper import (
    start_ba_session,
    get_beer_style_dict_from_json,
    get_beer_and_brewery_id,
    csv_beer_style_dict,
    create_beer_meta_dataframe,
    create_beer_rating_dataframe
)

@click.command()
@click.argument('input_filepath', type=click.Path(exists=True))
@click.argument('output_filepath', type=click.Path())
def main(input_filepath, output_filepath):
    """ Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """
    print(input_filepath)
    logger = logging.getLogger(__name__)
    logger.info('making final data set from raw data')

    data = {
          'login': os.environ.get("BEER_ADVOCATE_LOGIN"),
          'register': os.environ.get("BEER_ADVOCATE_REGISTER"),
          'password': os.environ.get("BEER_ADVOCATE_PASSWORD"),
          'cookie_check': os.environ.get("BEER_ADVOCATE_COOKIE_CHECK"),
          '_xfToken': os.environ.get("BEER_ADVOCATE_XFTOKEN"),
          'redirect': os.environ.get("BEER_ADVOCATE_REDIRECT"),
        }

    session = start_ba_session(data)
    beer_styles_dict_path = input_filepath + '/' + 'beer_styles_dict.json'
    beer_style_dict = get_beer_style_dict_from_json(beer_styles_dict_path)
    for beer_style, beer_sub_style_dict in beer_style_dict.items():
        for beer_sub_style, style_id in beer_sub_style_dict.items():
            beer_df = get_beer_and_brewery_id(beer_style, beer_sub_style, style_id, session)
            beer_style_ = beer_style.lower()
            beer_sub_style_ = beer_sub_style.lower()
            beer_sub_style_ = beer_sub_style_.replace(' - ', '_').replace(' *', '').replace('/', '').replace(' ', '_')
            style_id_ = str(style_id)
            style_path = beer_style_ + '/' + beer_sub_style_ + '_' + style_id_ + '.csv'
            beer_df.to_csv('s3://beer-recommendation-system-data/beer_data/beer_temp/' + style_path)
            beer_meta_df = create_beer_meta_dataframe(beer_df, session)
            beer_meta_df.to_csv('s3://beer-recommendation-system-data/beer_data/beer_meta/' + style_path)
            beer_rating_df = create_beer_rating_dataframe(beer_df, session)
            beer_rating_df.to_csv('s3://beer-recommendation-system-data/beer_data/beer_rating/' + style_path)


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
