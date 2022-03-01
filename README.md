# polly-python
This space is intended to help users for the following:-
1. Share example notebooks to answer frequently asked questions of Polly users and enable consumption. The notebooks can be opened directly on the Polly compute environment.
2. Raise issue for any feature requests or bugs they encounter while using polly-python
3. Share a high level roadmap about upcoming features on Polly-python library 

## How to add "Open in Polly" button in Polly notebooks
In Polly notebook after done with the relevent notebook. Follow the below steps
1. Open the terminal from Polly offerings
2. Run the following command

```
python3 -c "$(wget -q -O - https://raw.githubusercontent.com/ElucidataInc/polly-python/main/.github/workflows/notebook_download.py)"
```
3. Go to file explorer in Polly notebook under Polly offerings and select the newly created `ipynb` notebook.
4. Download the file by clicking on download button
5. After file is downloaded delete the created ipynb notebook 
