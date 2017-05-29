# Network request timeouts

If you are experiencing network request timeouts but the target node to send the request to is available and some requests are sent successfully, try increasing the request timeout in the DXRAM configuration file. This might happen if the system sends requests with huge payloads resulting in a higher transmission time and exceeding the set timeout.  You can find the entry
```JSON
"m_requestTimeout": {
    "m_value": 333,
    "m_unit": "ms"
}
```
under the *NetworkComponent* section.

Furthermore, this can also happen on very high loads (sending too many messages and the other node can't keep up with processing). Depending on your application/use case, you can ignore these errors or handle them accordingly by retrying the transmission.
