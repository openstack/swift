========================
Cross-domain Policy File
========================

A cross-domain policy file allows web pages hosted elsewhere to use client
side technologies such as Flash, Java and Silverlight to interact
with the Swift API.

See http://www.adobe.com/devnet/articles/crossdomain_policy_file_spec.html for
a description of the purpose and structure of the cross-domain policy
file. The cross-domain policy file is installed in the root of a web
server (i.e., the path is /crossdomain.xml).

The crossdomain middleware responds to a path of /crossdomain.xml with an
XML document such as::

    <?xml version="1.0"?>
    <!DOCTYPE cross-domain-policy SYSTEM "http://www.adobe.com/xml/dtds/cross-domain-policy.dtd" >
    <cross-domain-policy>
        <allow-access-from domain="*" secure="false" />
    </cross-domain-policy>

You should use a policy appropriate to your site. The examples and the
default policy are provided to indicate how to syntactically construct
a cross domain policy file -- they are not recommendations.

-------------
Configuration
-------------

To enable this middleware, add it to the pipeline in your proxy-server.conf
file. It should be added before any authentication (e.g., tempauth or
keystone) middleware. In this example ellipsis (...) indicate other
middleware you may have chosen to use::

    [pipeline:main]
    pipeline =  ... crossdomain ... authtoken ... proxy-server

And add a filter section, such as::

    [filter:crossdomain]
    use = egg:swift#crossdomain
    cross_domain_policy = <allow-access-from domain="*.example.com" />
        <allow-access-from domain="www.example.com" secure="false" />

For continuation lines, put some whitespace before the continuation
text. Ensure you put a completely blank line to terminate the
cross_domain_policy value.

The cross_domain_policy name/value is optional. If omitted, the policy
defaults as if you had specified::

    cross_domain_policy = <allow-access-from domain="*" secure="false" />


