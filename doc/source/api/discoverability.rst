===============
Discoverability
===============

Your Object Storage system might not enable all features that you read about because your service provider chooses which features to enable.

To discover which features are enabled in your Object Storage system,
use the ``/info`` request. However, your service provider might have
disabled the ``/info`` request, or you might be using an older version
that does not support the ``/info`` request.

To use the ``/info`` request, send a **GET** request using the ``/info``
path to the Object Store endpoint as shown in this example:

.. code:: console

    # curl https://storage.clouddrive.com/info

This example shows a truncated response body:

.. code:: console

    {
       "swift":{
          "version":"1.11.0"
       },
       "staticweb":{

       },
       "tempurl":{

       }
    }

This output shows that the Object Storage system has enabled the static
website and temporary URL features.

