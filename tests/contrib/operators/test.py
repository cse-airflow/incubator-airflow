
# TEST SCRIPT BELOW
def main():
    test_operator = AzureBatchAIOperator(
        'azure_batchai_default',
        'batch-ai-test-rg',
        'batch-ai-workspace',
        'batch-ai-cluster',
        'eastus',
        environment_variables={},
        volumes=[],
        task_id='test_operator'
        )
    print "testing execute of batch ai operator....."
    test_operator.execute()

if __name__ == '__main__':
    main()