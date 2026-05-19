/**
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 */
define(['N/search', 'N/record', 'N/runtime', 'N/format'],
    function (search, record, runtime, format) {
        var cachedDate18Formatted;

        function getDate18MonthsFormatted() {
            if (!cachedDate18Formatted) {
                var date18MonthsAgo = new Date();
                date18MonthsAgo.setMonth(date18MonthsAgo.getMonth() - 18);
                cachedDate18Formatted = format.format({
                    value: date18MonthsAgo,
                    type: format.Type.DATE
                });
            }
            return cachedDate18Formatted;
        }

        function getInputData() {
            var scriptObj = runtime.getCurrentScript();

            // Customer saved search id from script deployment parameter
            var customerSearchId = scriptObj.getParameter({
                name: 'custscript_customer_saved_search'
            });

            if (!customerSearchId) {
                log.error('CONFIG ERROR', 'custscript_customer_saved_search parameter is missing');
                return [];
            }

            log.debug("Script Parameter - Customer Saved Search", customerSearchId);

            var customerSearch = search.load({ id: customerSearchId });
            var totalCustomers = customerSearch.runPaged({ pageSize: 1000 }).count;

            log.audit('Input Search Ready', {
                savedSearchId: customerSearchId,
                totalCustomersFound: totalCustomers
            });

            return customerSearch;
        }

        function map(context) {
            var result = JSON.parse(context.value);
            var customerId = result.id || (result.values && result.values.internalid && result.values.internalid.value);

            if (!customerId) {
                log.error('MAP ERROR', 'Unable to resolve customer id from context.value');
                return;
            }

            context.write({
                key: customerId,
                value: '1'
            });
        }

        function reduce(context) {
            var customerId = context.key;

            try {
                // 2. LOAD CUSTOMER VALUES
                var custLookup = search.lookupFields({
                    type: search.Type.CUSTOMER,
                    id: customerId,
                    columns: ['balance', 'overduebalance']
                });

                // Extract values safely
                var balance = parseFloat(custLookup.balance) || 0;
                var overdue = parseFloat(custLookup.overduebalance) || 0;

                // 3. CHECK IF TODAY'S ENTRY EXISTS

                // 4. CREATE DAILY SNAPSHOT
                var snap = record.create({
                    type: 'customrecordbal_cust_update'
                });

                snap.setValue('custrecord1502', customerId);       // customer
                snap.setValue('custrecord1500', balance);          // balance
                snap.setValue('custrecord1504', overdue);          // overdue

                snap.save();

                // 5. GET HIGHEST BALANCE IN LAST 18 MONTHS
                var highSearch = search.create({
                    type: 'customrecordbal_cust_update',
                    filters: [
                        ['custrecord1502', 'is', customerId],
                        'AND',
                        ['custrecord1501', 'onorafter', getDate18MonthsFormatted()]
                    ],
                    columns: [
                        search.createColumn({ name: 'custrecord1500', sort: search.Sort.DESC }),
                        'custrecord1501'
                    ]
                });

                var highResult = highSearch.run().getRange({ start: 0, end: 1 });
                var customerUpdateValues = {
                    custentity_wf_bal_store: balance
                };

                if (highResult && highResult.length > 0) {
                    var highestBal = parseFloat(highResult[0].getValue('custrecord1500')) || 0;
                    var highestDate = highResult[0].getValue('custrecord1501');

                    // 6. UPDATE CUSTOMER FIELDS
                    customerUpdateValues.custentity_highestarbalance = highestBal;
                    customerUpdateValues.custentity_adc_high_bal_on = highestDate;
                }

                // Single submitFields call to reduce governance.
                record.submitFields({
                    type: record.Type.CUSTOMER,
                    id: customerId,
                    values: customerUpdateValues
                });

                context.write({
                    key: customerId,
                    value: 'processed'
                });
            } catch (e) {
                log.error('Reduce Error for customer ' + customerId, e);
                throw e;
            }
        }

        function summarize(summary) {
            var processedCount = 0;
            var errorCount = 0;

            if (summary.inputSummary.error) {
                log.error('Input Error', summary.inputSummary.error);
            }

            summary.reduceSummary.keys.iterator().each(function () {
                processedCount += 1;
                return true;
            });

            summary.reduceSummary.errors.iterator().each(function (key, error) {
                errorCount += 1;
                log.error('Reduce Error for customer ' + key, error);
                return true;
            });

            log.audit('Governance Summary', {
                usage: summary.usage,
                concurrency: summary.concurrency,
                yields: summary.yields
            });

            log.audit('Reduce Stage Count', {
                processedCustomers: processedCount,
                reduceErrors: errorCount
            });

            log.audit('END', 'Map/Reduce Processing Complete');
        }

        return {
            getInputData: getInputData,
            map: map,
            reduce: reduce,
            summarize: summarize
        };
    });