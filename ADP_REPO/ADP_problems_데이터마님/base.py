import os


def get_dataset_url(path):
    base_url = 'https://gitlab.com/jackmappotion/datasets/-/raw/main/ADP_datasets/ADP_problems_%EB%8D%B0%EC%9D%B4%ED%84%B0%EB%A7%88%EB%8B%98'
    url = os.path.join(base_url, path)
    return url
