const AWS = require("aws-sdk");
AWS.config.update( {
  region: "us-east-1"
});

const dynamodb = new AWS.DynamoDB.DocumentClient();
const dynamodbTableName = "fulfillmentapi";

const fulfillmentsPath = "/fulfillments";
const fulfillmentsParamPath = fulfillmentsPath + "/{orderID}";
const packingPath = fulfillmentsPath + "/packing";
const packingParamPath = packingPath + "/{orderID}";
const locationParamPath = fulfillmentsPath + "/location/{itemID}";
const shippingPath = fulfillmentsPath + "/shipping";
const shippingParamPath = shippingPath + "/{shippingID}";
const shippingLabelPath = fulfillmentsPath + "/shippingLabel";
const shippingLabelParamPath = shippingLabelPath + "/{orderID}";

exports.handler = async function(event) {
	console.log("Request event method: ", event.httpMethod);
	console.log("EVENT\n" + JSON.stringify(event, null, 2));
	let response;
	switch(true) {

		case event.httpMethod === "GET" && event.requestContext.resourcePath === fulfillmentsParamPath:
        	response = await getUser(event.pathParameters.orderID);
        	break;
		case event.httpMethod === "GET" && event.requestContext.resourcePath === shippingLabelParamPath:
			response = await getShippingLabelID(event.pathParameters.orderID);
			break;
		case event.httpMethod === "GET" && event.requestContext.resourcePath === locationParamPath:
			response = await getItemLocation(event.pathParameters.itemID);
			break;
		case event.httpMethod === "GET" && event.requestContext.resourcePath === packingParamPath:
			response = await getPacking(event.pathParameters.orderID);
			break;
	
		case event.httpMethod === "GET" && event.requestContext.resourcePath === fulfillmentsPath:
			if (event.queryStringParameters) {
				if (event.queryStringParameters.status !== undefined) {
					console.log("orderStatus is defined:", event.queryStringParameters.status);
					response = await getFulfillmentsQ(event.queryStringParameters.status);
				} else {
					console.log("orderStatus is undefined");
					response = await getFulfillments();
				}
			} else {
				console.log("No queryStringParameters in the event object");
				response = await getFulfillments();
			}
			break;

		case event.httpMethod === "POST" && event.requestContext.resourcePath === fulfillmentsPath:
		  	response = await saveFulfillment(JSON.parse(event.body));
			break;

		case event.httpMethod === "DELETE" && event.requestContext.resourcePath === fulfillmentsParamPath:
			response = await deleteOrder(event.pathParameters.orderID);
			break;

		case event.httpMethod === "PATCH" && event.requestContext.resourcePath === fulfillmentsParamPath:
			response = await changeStatus(event.pathParameters.orderID, JSON.parse(event.body).orderStatus);
			break;
			
		case event.httpMethod === "PATCH" && event.requestContext.resourcePath === packingParamPath:
			response = await removePackingQueue(event.pathParameters.orderID);
			break;

		case event.httpMethod === "PATCH" && event.requestContext.resourcePath === shippingParamPath:
			response = await removeShippingQueue(event.pathParameters.shippingID);
			break;

		case event.httpMethod === "DELETE" && event.requestContext.resourcePath === packingPath:
			response = await deleteStatus("PACKING");
			break;

		case event.httpMethod === "DELETE" && event.requestContext.resourcePath === shippingPath:
			response = await deleteStatus("SHIPPING");
			break;

		default:
			console.log("ERROR: NO MATCHING ENDPOINTS\n" + packingPath);
			response = buildResponse(404, event.requestContext.resourcePath);
  	}

	return response;
}

async function scanDynamoRecords(scanParams, itemArray) 
{
	try {
		const dynamoData = await dynamodb.scan(scanParams).promise();
		itemArray = itemArray.concat(dynamoData.Items);
		if (dynamoData.LastEvaluatedKey) {
			scanParams.ExclusiveStartkey = dynamoData.LastEvaluatedKey;
			return await scanDynamoRecords(scanParams, itemArray);
		}
		return itemArray;
	} 
	catch(error) {
		console.error('There was an error finding the item you requested.', error);
	}
}

function buildResponse(statusCode, body) {
	return {
		statusCode: statusCode,
		headers: {
			"Content-Type": "application/json"
		},
		body: JSON.stringify(body)
	}
}

//Get all fulfillments... but with a query variable
async function getFulfillmentsQ(queryValue) {
  console.log("getFulfillments(queryValue) has been entered");
	// Ensure queryValue is one of the three possible order statuses
	if (queryValue !== "FULFILLMENT" && queryValue !== "PACKING" && queryValue !== "SHIPPING") {
		// Bad query value, return 404
		return buildResponse(404, { message: "ERROR: Invalid query parameter value" });
	}

	const params = {
		TableName: dynamodbTableName,
		IndexName: "orderStatus-index",
		KeyConditionExpression: 'orderStatus = :skey', 
		ExpressionAttributeValues: {
			':skey': queryValue, 
		},
	};
	
	try { // Find all fulfillments within query parameter bounds
		const allFulfillments = await dynamodb.query(params).promise();
		const body = { // Generate a body
			order: allFulfillments.Items,
		};
		return buildResponse(200, body);
	} catch (error) { // Oops an error was made
		console.error("Error querying DynamoDB:", error);
		return buildResponse(500, { message: "Internal Server Error w/ getFulfillments(query)" });
	}
}

//Get all fulfillments
async function getFulfillments() {
  console.log("Entering getFulfillments()");
  const params = { // Define Tablename in this function
    TableName: dynamodbTableName
  };
  try {// Scan and load all of the dynamoDB's data, within parameters, to the allFulfillments variable
    const allFulfillments = await scanDynamoRecords(params, []);
    const body = { // Load all data into a body variable
      order: allFulfillments,
    };
    return buildResponse(200, body);
  } catch (error) { // Oops you made an error
    console.error("Error querying DynamoDB:", error);
    return buildResponse(500, { message: "Internal Server Error in getFulfillments()" });
  }
}


/**
 * Deletes an order from the DynamoDB table based on the provided orderID.
 *
 * @param {number|string} orderID - The ID of the order to be deleted. 
 *                                  This can be a number or a string that can be parsed into a number.
 * 
 * Function Workflow:
 * 1. Validates the orderID: Checks if orderID exists and is correctly formatted.
 *    - If invalid, returns a 404 response with an error message.
 * 
 * 2. Constructs the delete parameters with the orderID.
 *    - orderID is parsed into an integer and used to identify which order the user means to delete.
 * 
 * 3. Attempts to delete the order from the DynamoDB table.
 *    - Uses the AWS SDK's DynamoDB DocumentClient delete method.
 *    - If no attributes are returned (aka no order was found), a 404 response is returned.
 *    - If item is deleted, a 200 response is returned with details of the deleted item.
 * 
 * 4. Catches and handles any errors during the operation.
 *    - In case of exceptions, logs the error and returns a 500 internal server error response.
 * 
 * @returns {Object} A response object containing the status code and the body. 
 *                   The body includes a message and, if item is deleted, details of the deleted item.
 */
async function deleteOrder(orderID) 
{
  //Checks if orderID is blank or missing.
  if (!orderID || isNaN(orderID)) 
  {
    return buildResponse(404, { Message: "Invalid or missing orderID" });
  }

  //Gets desired order from table.
  const params = 
  {
    TableName: dynamodbTableName,
    Key: 
    {
      "orderID": parseInt(orderID), //Converts orderID to a number
    },
    ReturnValues: "ALL_OLD"
  };

  try 
  {
	//Retrieves response.
    const response = await dynamodb.delete(params).promise();

	//Returns 404 if item is not found.
    if (!response.Attributes) 
	{
      return buildResponse(404, { Message: "OrderID not found" });
    }

    //Response body.
    const body = 
    {
      Operation: "DELETE",
      Message: "SUCCESS",
      Item: response
    };

    //Return 200 response.
    return buildResponse(200, body);

  } 
  //Error response.
  catch (error) 
  {
    return buildResponse(500, { Message: "Internal Server Error" });
  }
}

/**
 * Updates the order status in the DynamoDB table for a specific order.
 * 
 * This function is specifically designed to update the status of orders that are currently
 * in the 'PACKING' phase to 'FULFILLMENT'. It will not update the order if it's not in the 'PACKING' queue.
 *
 * @param {number|string} orderID - The ID of the order to be deleted. 
 *                           		This can be a number or a string that can be parsed into a number.
 *
 * The function performs the following steps:
 * 1. Constructs the update parameters for the DynamoDB update operation.
 * 2. The `UpdateExpression` sets the new value of `orderStatus`.
 * 3. The `ConditionExpression` only updates if order is in packing queue.
 * 4. If the update is successful, a success response is returned.
 * 5. If the order is not in 'PACKING' status, catches the error and returns 404 response.
 * 6. All other errors caught, and a 500 internal server error response is returned.
 *
 * @returns {Object} - Returns an HTTP response object. If the update is successful, it returns a 
 *                     200 status code with a success message. If the order is not in 'PACKING' status, 
 *                     it returns a 404 reponse. For all other errors, it returns a 500 response.
 */
async function removePackingQueue(orderID) 
{
    const params = 
	{
        TableName: dynamodbTableName,
        Key: 
		{
            "orderID": parseInt(orderID)
        },
        UpdateExpression: "set orderStatus = :value",	//Creates update expression.
		ConditionExpression: "orderStatus = :currentValue", //Creates condition expression.
        ExpressionAttributeValues: 
		{
            ":value": "FULFILLMENT", //Updates orderStatus value.
			":currentValue": "PACKING" //Checks to make sure in packing queue.
        },
        ReturnValues: "UPDATED_NEW"
    };

    try 
	{
        const response = await dynamodb.update(params).promise();	//Updates order status.

		//Success body response.
        const body = 
		{
            Operation: "UPDATE",
            Message: "SUCCESS",
            UpdatedAttributes: response
        };
        return buildResponse(200, body);
    }
	catch (error) 
	{
		//Catches if order isn't in packing queue.
        if (error.code === 'ConditionalCheckFailedException')
		{
            console.error("Condition check failed: ", error);
            return buildResponse(404, { Message: "Update failed: Order not found in 'PACKING' queue. " });
        } 

		//Catches all other errors.
		else
		{
            console.error("Error updating order status: ", error);
            return buildResponse(500, { Message: "Internal Server Error" });
        }
    }
}

/**
 * Updates the order status from "SHIPPING" to "PACKING" in the DynamoDB table for a specific order.
 *
 * This function queries the table to find the attributes associated with the given shippingID.
 * It then updates the order status using the orderID associated with the given shippingID.
 *
 * @param {number | string} shippingID - The ID of the shipping order to be deleted. 
 *                           			 This can be a number or a string that can be parsed into a number.
 *
 * The function performs the following steps:
 * 1. Validates the shippingID and checks if it is a valid number.
 * 2. Constructs query parameters to search for the primary keys associated with the shippingID.
 * 3. Executes a query operation on the DynamoDB table.
 * 4. Returns a 404 error if no attributes are found in response.
 * 5. Updates order status to be "PACKING" from "SHIPPING".
 * 6. Returns a success response upon successful update of all items.
 * 7. Returns error response if any errors are caught.
 * 
 * @returns {Object} - Returns an HTTP response object. If the shippingID is invalid or missing, it returns
 *                     a 404 status code with an error message. If the shippingID is valid but not found in the
 *                     database, it also returns a 404 status code with a different message. If the items are
 *                     successfully updated, it returns a 200 status code with a success message. For all other
 *                     errors, it returns a 500 status code indicating an internal server error.
 */
async function removeShippingQueue(shippingID) 
{
	//Checks if shippingID is blank or missing.
	if (!shippingID || isNaN(shippingID)) 
	{
		return buildResponse(404, { Message: "Invalid or missing shippingID" });
	}

    //Query params to find associated orderID of shippingID.
    const query_params = 
	{
        TableName: dynamodbTableName,
        IndexName: "shippingID-index",
        KeyConditionExpression: 'shippingID = :nkey',
        ExpressionAttributeValues: 
		{
            ':nkey': parseInt(shippingID),
        },
    };

    try 
	{
		//Finds order.
        const queryResult = await dynamodb.query(query_params).promise();

        //Check if any item was found
        if (queryResult.Items.length === 0) 
		{
            return buildResponse(404, { Message: "shippingID not found" });
        }

        //Goes through all items found (should be one).
        for (const item of queryResult.Items) 
		{
			//Parameters to update of item.
            const update_params = 
			{
                TableName: dynamodbTableName,
                Key: 
				{ 
					orderID: item.orderID 
				},
				UpdateExpression: "set orderStatus = :value",	//Creates update expression.
				ConditionExpression: "orderStatus = :currentValue", //Creates condition expression.
				ExpressionAttributeValues: 
				{
					":value": "PACKING", //Updates orderStatus value.
					":currentValue": "SHIPPING" //Checks to make sure in shipping queue.
				},
				ReturnValues: "UPDATED_NEW"
            };

            const response = await dynamodb.update(update_params).promise();	//Updates order status.

			//Success body response.
			const body = 
			{
				Operation: "UPDATE",
				Message: "SUCCESS",
				UpdatedAttributes: response
			};
			return buildResponse(200, body);
        }

    } 
	catch (error) 
	{
		//Catches if order isn't in shipping queue.
		if (error.code === 'ConditionalCheckFailedException')
		{
			console.error("Condition check failed: ", error);
			return buildResponse(404, { Message: "Update failed: Order not found in 'SHIPPING' queue. " });
		} 
		else
		{
			console.error("Error in deleteShippingOrder:", error);
			return buildResponse(500, { message: "Internal Server Error" });
		}
    }
}

async function saveFulfillment(requestBody) {
	// Force shippingID to be zero
	requestBody['shippingID'] = 0;

	const params = 
	{
		TableName: dynamodbTableName,
		Item: requestBody
	}
	
	// Error check the requestBody to ensure it meets specs
	try {
		await validateOrderID(requestBody.orderID);
	} catch (error) {
		let message = "FAILURE: Order with supplied orderID already exists";
		// Construct error message and return 404 error
		const body = {
			message: message 
		}
		return buildResponse(404, body);
	}
	return await dynamodb.put(params).promise().then(() => {
		// Return a 200 status code on success
		return buildResponse(200, { message: "Success" });
	}, (error) => {
		if (error === 500) {
			const body = {
				message: "FAILURE: Unexpected error occurred"
			}
			return buildResponse(500, body);
		}
	})
}

// Get the order with the specified orderID
async function getUser(orderID) {

    const params = {
        TableName: dynamodbTableName,
        Key: {
			// parse int because it is declared as a number in the table
            "orderID": parseInt(orderID),
        }
    };

    try {
        const response = await dynamodb.get(params).promise();

		// Return 404 if orderID is not found
        if (!response.Item) {
            return buildResponse(404, { error: 'Invalid order id.'});
        }

		// Return 200 if orderID is found
        return buildResponse(200, response.Item);
        
    } catch (error) {
        console.error("Error: ", error);
		// Return 500 if unexpected error occurs
        return buildResponse(500, { error: 'Sorry, something unexpected happened while trying to process your request.' });
    }
}
/**
 * Gets the orderID for packing
 * 
 * @param {number | string} orderID Packing ID to be retrieved AND string that will parsed into int
 * @returns 
 * 		500 - Internal Server Error
 * 		404 - Order not found
 * 		200	- Order found
 */
async function getPacking(orderID) {
    const params = {
        TableName: dynamodbTableName,
        Key: {
            "orderID": parseInt(orderID)
        }
    };

    try {
        const response = await dynamodb.get(params).promise();

        //Gets boxSize and boxWeight from order.
        const boxSize = response.Item?.boxSize;
        const boxWeight = response.Item?.boxWeight;

        //Checks to see if both size and weight were found.
        if (boxSize && boxWeight) 
		{
            return buildResponse(200, { boxSize, boxWeight });
        }
		//Otherwise, returns error.
		else 
		{
            return buildResponse(404, { Message: "Order not found or box details missing" });
        }
    }
	catch (error) 
	{
        console.error("Error fetching order details: ", error);
        return buildResponse(500, { Message: "Internal Server Error" });
    }
}

/**
 * Gets the itemID for location
 * 
 * @param {number | string} itemID Location itemID to be retrieved AND string that will parsed into int
 * @returns 
 * 		500 - Internal Server Error
 * 		404 - Order not found
 * 		200	- Order found
 */
async function getItemLocation(itemID) 
{
	//Makes sure itemID isn't null or undefined.
    if (itemID == null) 
	{
        return buildResponse(400, {Message: "Invalid itemID provided."});
    }
	
    //Converts itemID to a string for comparison.
    itemID = itemID.toString();

	//Params to scan entire table.
    const scanParams = 
	{
        TableName: dynamodbTableName
    };

    try 
	{
		//Scans whole table.
        const items = await scanDynamoRecords(scanParams, []);

        let matchedItem = null;

		//Goes through all items.
        for (const order of items) 
		{
			//Checks that there is an order list.
            if (order.inventoryList) 
			{
				//Compares itemID to the items in the table.
                const foundItem = order.inventoryList.find(item => item.itemID && item.itemID.toString() === itemID);

				//Stores the item if matching.
                if (foundItem) 
				{
                    matchedItem = foundItem;
                    break;
                }
            }
        }

		//No matching items found.
        if (!matchedItem) 
		{
            return buildResponse(404, { Message: "Item not found." });
        }

		//Returns matching item.
        return buildResponse(200, { matchedItem });
    }

	//Error response.
	catch (error) 
	{
        console.error("Error scanning DynamoDB for item location:", error);
        return buildResponse(500, { Message: "Internal Server Error." });
    }
}



/**
 * Gets the orderID for shipping label, return a random number for label, and modify the shippingID attribute
 * 
 * @param {number | string} orderID Shipping label for orderID to be retrieved AND string that will parsed into int
 * @returns 
 * 		500 - Internal Server Error
 * 		404 - Order not found
 * 		200	- Order found
 */
async function getShippingLabelID(orderID) {
    // Order ID validation
    if (!orderID || isNaN(orderID)) {
        return buildResponse(400, { Message: "Invalid or missing orderID" });
    }

    // Check if the orderID exists
    const orderExistsParams = {
        TableName: dynamodbTableName,
        Key: {
            "orderID": parseInt(orderID)
        }
    };

    try {
        const orderExistsResponse = await dynamodb.get(orderExistsParams).promise();

        if (!orderExistsResponse.Item) {
            return buildResponse(404, { Message: "Order not found." });
        }

        // Check if the order already has a shippingID
        if (orderExistsResponse.Item.shippingID === undefined) 
		{
			return buildResponse(409, { Message: "Undefined shippingID." });
        }
		
		//Checks if order is still default 0.
		else if(orderExistsResponse.Item.shippingID !== 0)
		{
			return buildResponse(409, { Message: "OrderID already has a shippingID." });
		}

        // Ensure the output is between 4 and 9 digits
        const minDigits = 4;
        const maxDigits = 9;
        const randomShippingID = Math.floor(Math.random() * (Math.pow(10, maxDigits) - Math.pow(10, minDigits)) + Math.pow(10, minDigits));

        // Update the order with the random shipping ID
        const updateParams = {
            TableName: dynamodbTableName,
            Key: {
                "orderID": parseInt(orderID)
            },
            UpdateExpression: "SET shippingID = :value",
            ExpressionAttributeValues: {
                ":value": randomShippingID,
				":zero": 0
            },
			// Ensure the shippingID does not already exist
            ConditionExpression: "attribute_not_exists(shippingID) OR shippingID = :zero", 
            ReturnValues: "UPDATED_NEW"
        };

        const response = await dynamodb.update(updateParams).promise();

        // Triple checking the update was successful
        if (!response.Attributes) {
            return buildResponse(409, { Message: "Conflict: ShippingID already exists." });
        }

        // Successful update
        const body = {
            Operation: "UPDATE",
            Message: "SUCCESS",
            UpdatedAttributes: response
        };
        return buildResponse(200, body);
    } 
	catch (error) {
        console.error("Error updating shipping ID: ", error);
        return buildResponse(500, { Message: "Internal Server Error" });
    }
}


async function validateOrderID(orderID) {
	// Check to ensure no other orders with orderID exist
	let checkParams = {
	  TableName: dynamodbTableName,
	  KeyConditionExpression: 'orderID = :nkey', 
	  ExpressionAttributeValues: {
		':nkey': orderID,
	  }
	}

	let query = await dynamodb.query(checkParams).promise();
	let data = query.Items;

	// Length > 0 indicates at least 1 item with same orderID already exists
	if (data.length !== 0) {
		throw Error("orderID");
	}
}

async function deleteStatus(orderStatus) {
	try {
		// Get all items with orderStatus as the order status
		let orders = await getFulfillmentsQ(orderStatus);
		orders = JSON.parse(orders.body);

		// Return 500 error if no orders to delete
		if (orders['order'].length === 0) {
			return buildResponse(500, { message: `ERROR: No ${orderStatus} orders to delete.` });
		}

		// Build a delete request object for every matching order
		let toDelete = [];
		let batchReturn = [];
		for (const item of orders.order) {
			const request = {
				DeleteRequest: {
					Key: {
						orderID: item.orderID
					}
				}
			}
			// Add delete request to an array
			toDelete.push(request);

			// batchWrite only works with up to 25 requests, so go ahead and 
			// delete if 25 requets reached
			if (toDelete.length == 25) {
				// Build batchWrite parameters
				let params = {
					RequestItems: {
						[dynamodbTableName]: toDelete
					}
				}
				batchReturn.push(await dynamodb.batchWrite(params).promise());
				// Reset toDelete
				toDelete = [];
			}
		}
		// If toDelete.length === 0, means exactly some multiple of 25 orders were
		// already deleted, so just return a 200 response
		if (toDelete.length === 0) {
			return buildResponse(200, {items: batchReturn});
		}

		// Build batchWrite parameters
		let params = {
			RequestItems: {
				[dynamodbTableName]: toDelete
			}
		}

		// Delete all matching orders
		batchReturn.push(await dynamodb.batchWrite(params).promise());
		
		// If here, successfully deleted all orders with supplied orderStatus
		const body = {
			Operation: "DELETE",
			Message: `Successfully deleted all ${orderStatus} orders`
		}
		return buildResponse(200, body);
	} catch (error) {
		const body = {
			message: "ERROR: Internal server error or there are no items to delete"
		};
		return buildResponse(500, body);
	}
}

// Update an Order with a valid orderStatus (newStatus)
async function changeStatus(orderID, newStatus) {
    const params = {
        TableName: dynamodbTableName,
        Key: { // Define and grab orderID key
            orderID: parseInt(orderID),
        }, 
        UpdateExpression: 'set orderStatus = :s',
        ExpressionAttributeValues: { // Assign  newStatus
            ':s': newStatus,
        },
        ReturnValues: 'UPDATED_NEW',
    };

	// Ensure order actually exists
	// Will result in an error if this is the case
	try {
		await validateOrderID(parseInt(orderID));
		// If here, order does not exist, so return 404
		const body = {
			message: "FAILURE: No order found with supplied orderID"
		}
		return buildResponse(404, body);
	} catch (error) {
		// Do nothing if an error occurs since this means the order exists
	}

    try { // Generate a reponse...if it can't throw an error
        const response = await dynamodb.update(params).promise();

        // Check if Attributes property is present before accessing it
        const updatedItem = response.Attributes || {};

        return buildResponse(200, { message: "Success", updatedItem });
    } catch (error) { // Oops made a mistake was made
        console.error("Error in changeStatus:", error);
        const statusCode = error.statusCode || 500;
        const body = {
            message: `FAILURE: ${error.message || "Unexpected error occurred"}`,
        };

        return buildResponse(statusCode, body);
    }
}
