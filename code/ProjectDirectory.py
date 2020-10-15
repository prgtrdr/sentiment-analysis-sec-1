import os
from pathlib import Path



def get_project_dir():
    try:
        project_dir = Path.cwd() / '/' / 'Users' / 'bill' / 'Documents' / 'Development' / 'sentiment-analysis-sec'
        os.chdir(project_dir)
    except BaseException as e:
        project_dir = Path.cwd() / '/' / 'Volumes' / 'GoogleDrive' / 'My Drive' / 'Jotham' / 'Personal Docs' / 'ML for finance' / 'SEC Sentiment Analysis - Github Upload' / 'sec-sentiment'
        os.chdir(project_dir)
    return project_dir
