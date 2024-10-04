import json
import boto3
import matplotlib.pyplot as plt
from io import BytesIO
import pandas as pd

s3_client = boto3.client('s3')
bedrock_client = boto3.client('bedrock', region_name='ap-south-1')  # Initialize AWS Bedrock client with your 
region

class DataIngestionAgent:
    def __init__(self, file_path):
        self.file_path = file_path

    def read_data(self):
        stock_data = pd.read_csv(self.file_path)
        stock_data['Date'] = pd.to_datetime(stock_data['Date'])
        return stock_data

class PlottingAgent:
    def __init__(self, stock_data):
        self.stock_data = stock_data

    def plot_and_upload(self, bucket_name):
        end_date = self.stock_data['Date'].max()
        start_date = end_date - pd.DateOffset(months=3)
        filtered_data = self.stock_data[(self.stock_data['Date'] >= start_date) & (self.stock_data['Date'] <= 
end_date)]

        stocks = filtered_data['Stock'].unique()
        plot_files = []

        for stock in stocks:
            stock_prices = filtered_data[filtered_data['Stock'] == stock]
            plt.figure(figsize=(10, 5))
            plt.plot(stock_prices['Date'], stock_prices['Price'], label=stock)
            plt.xlabel('Date')
            plt.ylabel('Price')
            plt.title(f'Stock Prices for {stock} over Last 3 Months')
            plt.legend()
            plt.grid(True)

            # Save plot to a BytesIO object
            plot_buffer = BytesIO()
            plt.savefig(plot_buffer, format='png')
            plot_buffer.seek(0)

            # Upload the plot to S3
            plot_key = f'plots/{stock}_3_months.png'
            s3_client.upload_fileobj(plot_buffer, bucket_name, plot_key)
            plot_files.append(plot_key)

            plt.close()

        return plot_files

class AnalysisAgent:
    def __init__(self, plot_files, bucket_name):
        self.plot_files = plot_files
        self.bucket_name = bucket_name

    def analyze_plots(self):
        stock_performance = {}

        for plot_key in self.plot_files:
            stock_name = plot_key.split('/')[-1].split('_')[0]
            plot_url = f's3://{self.bucket_name}/{plot_key}'
            prompt = (
                f"Analyze the stock plot located at {plot_url}. The plot represents the stock price "
                f"of {stock_name} over the last three months. Consider the following aspects while analyzing the plot: "
                f"1. The overall trend direction (upward, downward, or stable) of the stock price. "
                f"2. The volatility of the stock price (high or low fluctuations). "
                f"3. The overall performance based on the visual data. "
                f"4. Potential future growth or decline inferred from the trend. "
                f"Provide a score between 0 and 10, 0 being lowest performing stock and 10 best performing stock "
                f"based on performance for this stock based on the visual data in the plot."
            )

            response = bedrock_client.invoke_model(
                ModelId='anthropic.claude-3-sonnet-20240229-v1:0',  # Replace with your model ID
                ContentType='text/plain',
                Accept='application/json',
                Body=prompt
            )

            result = response['Body'].read().decode('utf-8')
            performance_metric = self.parse_response(result)
            stock_performance[stock_name] = performance_metric

        sorted_stocks = sorted(stock_performance.items(), key=lambda x: x[1], reverse=True)
        top_stocks = [stock[0] for stock in sorted_stocks[:3]]
        return top_stocks

    def parse_response(self, response):
        # Implement logic to parse the response from the model
        # Assuming the response contains a performance metric in a structured format
        try:
            response_json = json.loads(response)
            performance_metric = response_json.get('performance_metric', 0)
            return performance_metric
        except json.JSONDecodeError:
            return 0  # Default to 0 if response is not in expected format

def lambda_handler(event, context):
    try:
        s3_bucket = event['s3_bucket']
        s3_key = event['s3_key']

        # Download file from S3
        file_path = f'/tmp/{s3_key.split("/")[-1]}'
        s3_client.download_file(s3_bucket, s3_key, file_path)

        # Data ingestion
        data_agent = DataIngestionAgent(file_path)
        stock_data = data_agent.read_data()

        # Plotting and uploading to S3
        plotting_agent = PlottingAgent(stock_data)
        plot_files = plotting_agent.plot_and_upload(s3_bucket)

        # Analysis using AWS Bedrock
        analysis_agent = AnalysisAgent(plot_files, s3_bucket)
        top_stocks = analysis_agent.analyze_plots()

        return {
            'statusCode': 200,
            'body': json.dumps({'top_stocks': top_stocks})
        }
    except:
        return {
            'message': 'Not able to process',
        }


