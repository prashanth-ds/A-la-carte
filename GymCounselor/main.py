import pandas as pd
import warnings
import plotly.graph_objs as go
import csv

warnings.filterwarnings("ignore")


class SessionEventsAnalysis:

    """
    This Class projects the users and their session details.

    Attributes
        file_path : str
            Relative/Absolute path of the required Excel file.

    All tasks in GC Tasks.doc file

    Task 1:
        Table - events
        Schema
        - visitor_id integer not null
        - session_id integer not null
        - session_start datetime
        - session_end datetime
        - page_Visited varchar(255)

        Query- "SELECT ee.visitor_id, ee.session_id, ee.session_start, ee.session_end FROM events ee
                WHERE CONCAT(ee.visitor_id, ee.session_id)
                IN
                (SELECT CONCAT(e.visitor_id, e.session_id) FROM events e WHERE e.page_Visited='pricing')
                AND
                TIMESTAMPDIFF(SECOND, session_end, session_start) > 45
                GROUP by ee.visitor_id, ee.session_id HAVING COUNT(*)=2;"

    """

    def __init__(self, file_path):
        '''
        Here the path of the excel file and present sheets are declared.

        :param file_path: GC_Task_Sheet.xlsx
        '''
        self.xl_sheets = file_path
        self.user_data_sheet = pd.read_excel(file_path, sheet_name='user_data')
        self.session_data_sheet = pd.read_excel(file_path, sheet_name='session_data')
        self.session_sum_sheet = pd.read_excel(file_path, sheet_name='session_sum')
        self.event_data_sheet = pd.read_excel(file_path, sheet_name='event_data')

    def to_csv(self, send=False):
        """
        - Use any programming tools necessary to get only visitor_id, session_id, country, event_date and
        signup_start onto a dataframe. Sort by ascending event_dates. Select only those rows where Experiment
        Number=10000, and event_date lies in 2021/05/03-2021/05/09 (inclusive) range. Export this dataframe onto a csv.

        :param send: bool
            This is only for static purpose within the class.
        :return: Filtered Dataframe with all constraints.
        """

        # Here we restrict the columns with Experiment Number 10000.
        self.user_data_sheet = self.user_data_sheet[self.user_data_sheet['Experiment Number'] == 10000]
        users_sessions_merge = pd.merge(self.user_data_sheet, self.session_data_sheet, on='session_id')

        # Here we define the inclusive data range as mentioned in the question.
        users_sessions_merge = users_sessions_merge[(users_sessions_merge['event_date'] >= '2021-05-03') & (users_sessions_merge['event_date'] <= '2021-05-09')]
        columns = ['visitor_id', 'session_id', 'country', 'event_date', 'signup_start']
        filtered = users_sessions_merge[columns]
        filtered.index.rename("Sl.no", inplace=True)

        # Here we Export the required data to User and Session Data.csv
        filtered.to_csv("UserSessionData.csv")
        if send is True:
            # Used for static purpose.
            return filtered

    def count_rows(self):
        """
        - Refer to the “session_data” sheet in the Excel sheet given. Create a function that counts the number of
        rows where value=1 on a per day basis, for each column.
        """

        users_sessions_merge = pd.merge(self.user_data_sheet, self.session_data_sheet, on='session_id')

        # We store the dates column separately
        dates = users_sessions_merge['event_date']

        # These are the columns to be used to count value 1 on daily basis.
        columns = ['is_rp', 'signup_start', 'signup_complete', 'active_after_7d']

        row_count_cols = []
        for i in columns:
            # Here we filter each column to have just value 1 and append each column separately to a list.
            row_count_cols.append(users_sessions_merge[i][users_sessions_merge[i] == 1])

        counts = []
        for rows in row_count_cols:
            # Here we count number of rows for each column on daily basis and append it to a list.
            rows['event_date'] = dates
            counts.append(rows.groupby('event_date').count())
        daily_count = counts[0]  # This initialization for the merge.
        for i in range(len(counts) - 1):
            # Here we merge all columns to form a single DataFrame on basis of event_date
            daily_count = pd.merge(daily_count, counts[i + 1], on='event_date')

        daily_count.to_csv("RowCount.csv")
        print('Row Count of each column on every day')
        print(daily_count)

    def column_ratio(self):
        """
        - Let’s assume that after some data manipulation, you get data which is present in the sheet “session_sum”.
        Write a function, which finds out the ratio of a column with respect to its previous one (for example,
        col2/col1, col3/col2 and so on). Export the result into a separate csv.
        """

        ratios = [None]  # Since First column has no basis here for taking ratios, we initialize it as None.
        for i in range(len(self.session_sum_sheet.columns) - 1):
            #  Here we divide/take ratio of two consecutive columns like (col2/col1, col3/col2 and so on).
            ratios.append(self.session_sum_sheet[self.session_sum_sheet.columns[i + 1]][0] / self.session_sum_sheet[self.session_sum_sheet.columns[i]][0])

        # Here we append the generated ratios to the DataFrame.
        self.session_sum_sheet.loc[len(self.session_sum_sheet.index)] = ratios

        # Here we Export the generated data to the csv file.
        self.session_sum_sheet.to_csv("ColumnRatio.csv")

    def visitors_graph(self):
        """
        - Write a function that plots a graph (using any module of your choice) which shows the number of visitors 
        per day. 

        """

        # Here we retrieve the filtered data generated from static to_csv() function and filter it for number of 
        # visitors on each day mentioned. 
        visitors_data = self.to_csv(send=True).groupby("event_date").count()['visitor_id']
        data = go.Bar(x=visitors_data.index, y=visitors_data, name="No. of visitors")
        layout = go.Layout(
            title="Visitors Count",
            xaxis=go.layout.XAxis(
                title="Event Date"
            ),
            yaxis=go.layout.YAxis(
                title="No. of Visitors"
            )
        )
        figure = go.Figure(data=[data], layout=layout)
        figure.show()

    def payload_segregation(self):
        """
        - 4. Refer to the ‘event_data’ sheet given. The payload column contains keys=value pairs separated by ‘&’.
        Make a function that exports a csv which expands the data in the following format:

        """

        # Here we split the data from the sheet for segregation
        event_ts = self.event_data_sheet.iloc[0]['evnt_ts']
        visitor_id = self.event_data_sheet.iloc[0]['visitor_id']
        payload_id = str(self.event_data_sheet.iloc[0]['payload_column'])

        # Firstly Split it by '&' as that's what differentiates key-value pairs in the URL.
        pair = payload_id.split("&")
        segregated_data = {}
        for i in pair:
            # Create a dictionary of key value pairs by splitting the attributes generated from previous step.
            key_val = i.split("=")
            segregated_data[key_val[0]] = key_val[1]

        detailed_data = []
        for key, value in segregated_data.items():
            # Here we make a 2D list for inserting the data into the csv file.
            detailed_data.append([event_ts, visitor_id, key, value])

        column_names = ['evnt_ts', 'visitor_id', 'payload_key', 'payload_val']

        with open('event_data.csv', 'w') as file:
            # Create the writer object and insert all the rows.
            writer = csv.writer(file)
            writer.writerow(column_names)
            writer.writerows(detailed_data)


solution = SessionEventsAnalysis("GC_Task_Sheet.xlsx")
print(solution.__doc__)
solution.to_csv()
solution.count_rows()
solution.column_ratio()
solution.visitors_graph()
solution.payload_segregation()




