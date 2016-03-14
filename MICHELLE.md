1. Open a terminal.
2. cd to your repository:

        $ cd ~/de_id

3. Activate your virtual environment:

        $ workon de_id

4. Copy your person_course file to the `private_data` directory.

        $ cp ~/Desktop/Thesis\ Datasets/2_01x_2013/sql/MITx__2_01x__2013_Spring_latest_person_course_data.txt private_data/

5. Convert the file from tab-separated (TSV) to comma-separated (CSV):

        $ python tsv_to_csv.py < private_data/MITx__2_01x__2013_Spring_latest_person_course_data.txt > private_data/input.csv

6. Start the server:

        $ jupyter notebook

7. After the browser window appears, click `De-identification.ipynb`.
8. Select `Cell > Run All`. This will start the code running. You should see asterisks beside cells as they run. There
 will be a cell toward the middle that asks for your input.
