## Main files
- **Notebook** (pandas_consumption.ipynb): This is the main file containing all the implemented code, along with explanations. It includes images, dashboards, and additional information.
- **Deploy script** (deploy_script.py): This script is designed to be executed from the command line.
	- Usage: The script can be invoked from the command line, supporting three output types: local, s3, or pg. Example usage is shown below:
		sergio.zapata:~/workspace/prueba/ZapataSergio_Challenge$ python deploy_script.py --output-type local

## Other assets
- Images: These are referenced and displayed within the notebook.
- JSON (transactions.json): Response from the API (created in the notebook).
- "dataset" folder: Contains the processed pages, obtained from the JSON response.
