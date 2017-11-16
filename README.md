# Simple node.js app using streamdata.io
This application will start a Streamdata client to get data and updates from the specified URL. On reception of data, this application will forward it to an HTTP Rest endpoint.

## Step by step setup

1. Create an free account on streamdata.io https://portal.streamdata.io/#/register to get an App token.

2. Edit server.js and replace ```[YOUR_STREAMDATAIO_APP_TOKEN]``` with your App token.

3. Make sure you have Node version 0.11.5 or later installed

4. Install node dependencies by running in a terminal:

  ```
  npm install
  ```

5. Launch node in a terminal:

  ```
  node server.js
  ```  

You should see data and patches pushed in your application and displayed on your terminal.

you can use the provided demo example API which simulates updating stocks prices from a financial market:
'http://stockmarket.streamdata.io/v2/prices'

Feel free to test it with any REST/Json API of your choice.
