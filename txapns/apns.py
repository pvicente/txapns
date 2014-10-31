'''
Created on Jun 10, 2013

@author: pvicente
'''
from __future__ import with_statement
from OpenSSL import SSL, crypto
from encoding import StringIO
from twisted.application import service
from twisted.internet import reactor, defer
from twisted.internet.protocol import ReconnectingClientFactory, ClientFactory, \
    Protocol
from twisted.internet.ssl import ClientContextFactory
from twisted.protocols.basic import LineReceiver
from twisted.python import log
from zope.interface import Interface, implements
import binascii

APNS_SERVER_SANDBOX_HOSTNAME = "gateway.sandbox.push.apple.com"
APNS_SERVER_HOSTNAME = "gateway.push.apple.com"
APNS_SERVER_PORT = 2195
FEEDBACK_SERVER_SANDBOX_HOSTNAME = "feedback.sandbox.push.apple.com"
FEEDBACK_SERVER_HOSTNAME = "feedback.push.apple.com"
FEEDBACK_SERVER_PORT = 2196

app_ids = {} # {'app_id': APNSService()}

def log_errback(name):
    def _log_errback(err, *args):
        log.err('errback in %s : %s' % (name, str(err)))
        return err
    return _log_errback

class IAPNSService(Interface):
        """ Interface for APNS """
        
        def write(self, notification):
                """ Write the notification to APNS """
        
        def read(self):
                """ Read from the feedback service """


class APNSClientContextFactory(ClientContextFactory):
    def __init__(self, ssl_cert_file):
        if 'BEGIN CERTIFICATE' not in ssl_cert_file:
            log.msg('APNSClientContextFactory ssl_cert_file=%s' % ssl_cert_file)
        else:
            log.msg('APNSClientContextFactory ssl_cert_file={FROM_STRING}')
        self.ctx = SSL.Context(SSL.TLSv1_METHOD)
        if 'BEGIN CERTIFICATE' in ssl_cert_file:
            cer = crypto.load_certificate(crypto.FILETYPE_PEM, ssl_cert_file)
            pkey = crypto.load_privatekey(crypto.FILETYPE_PEM, ssl_cert_file)
            self.ctx.use_certificate(cer)
            self.ctx.use_privatekey(pkey)
        else:
            self.ctx.use_certificate_file(ssl_cert_file)
            self.ctx.use_privatekey_file(ssl_cert_file)
    
    def getContext(self):
        return self.ctx


class APNSProtocol(Protocol):
    def connectionMade(self):
        log.msg('APNSProtocol connectionMade')
        self.factory.addClient(self)
    
    def sendMessage(self, msg):
        #log.msg('APNSProtocol sendMessage msg=%s' % binascii.hexlify(msg))
        return self.transport.write(msg)
    
    def connectionLost(self, reason):
        log.msg('APNSProtocol connectionLost %s' %(reason))
        self.factory.removeClient(self)


class APNSFeedbackHandler(LineReceiver):
    MAX_LENGTH = 1024*1024
    
    def connectionMade(self):
        log.msg('feedbackHandler connectionMade')

    def rawDataReceived(self, data):
        log.msg('feedbackHandler rawDataReceived %s' % binascii.hexlify(data))
        self.io.write(data)
    
    def lineReceived(self, data):
        log.msg('feedbackHandler lineReceived %s' % binascii.hexlify(data))
        self.io.write(data)

    def connectionLost(self, reason):
        log.msg('feedbackHandler connectionLost %s' % reason)
        self.deferred.callback(self.io.getvalue())
        self.io.close()


class APNSFeedbackClientFactory(ClientFactory):
    protocol = APNSFeedbackHandler
    
    def __init__(self):
        self.deferred = defer.Deferred()
    
    def buildProtocol(self, addr):
        p = self.protocol()
        p.factory = self
        p.deferred = self.deferred
        p.io = StringIO()
        p.setRawMode()
        return p
    
    def startedConnecting(self, connector):
        log.msg('APNSFeedbackClientFactory startedConnecting')
    
    def clientConnectionLost(self, connector, reason):
        log.msg('APNSFeedbackClientFactory clientConnectionLost reason=%s' % reason)
        ClientFactory.clientConnectionLost(self, connector, reason)
    
    def clientConnectionFailed(self, connector, reason):
        log.msg('APNSFeedbackClientFactory clientConnectionFailed reason=%s' % reason)
        ClientFactory.clientConnectionLost(self, connector, reason)


class APNSClientFactory(ReconnectingClientFactory):
    protocol = APNSProtocol
    
    def __init__(self):
        self._deferred=[] 
        self.clientProtocol = None
    
    @property
    def deferred(self):
        d = defer.Deferred()
        self._deferred.append(d)
        return d
    
    def addClient(self, p):
        self.clientProtocol = p
        for d in self._deferred:
            d.callback(p)
        self._deferred=[]
    
    def removeClient(self, p):
        self._deferred = []
        self.clientProtocol = None
    
    def startedConnecting(self, connector):
        log.msg('APNSClientFactory startedConnecting')
    
    def buildProtocol(self, addr):
        self.resetDelay()
        p = self.protocol()
        p.factory = self
        return p
    
    def clientConnectionLost(self, connector, reason):
        log.msg('APNSClientFactory clientConnectionLost reason=%s' % reason)
        ReconnectingClientFactory.clientConnectionLost(self, connector, reason)
    
    def clientConnectionFailed(self, connector, reason):
        log.msg('APNSClientFactory clientConnectionFailed reason=%s' % reason)
        ReconnectingClientFactory.clientConnectionLost(self, connector, reason)


class APNSService(service.Service):
    """ A Service that sends notifications and receives 
    feedback from the Apple Push Notification Service
    """
    
    implements(IAPNSService)
    clientProtocolFactory = APNSClientFactory
    feedbackProtocolFactory = APNSFeedbackClientFactory
    
    def __init__(self, cert_path, environment, timeout=15):
        log.msg('APNSService __init__')
        self.factory = None
        self.environment = environment
        self.cert_path = cert_path
        self.raw_mode = False
        self.timeout = timeout
    
    def getContextFactory(self):
        return APNSClientContextFactory(self.cert_path)
    
    def write(self, notifications):
        "Connect to the APNS service and send notifications"
        if not self.factory:
            log.msg('APNSService write (connecting)')
            server, port = ((APNS_SERVER_SANDBOX_HOSTNAME 
                                            if self.environment == 'sandbox'
                                            else APNS_SERVER_HOSTNAME), APNS_SERVER_PORT)
            self.factory = self.clientProtocolFactory()
            context = self.getContextFactory()
            reactor.connectSSL(server, port, self.factory, context)
        
        client = self.factory.clientProtocol
        if client:
            return client.sendMessage(notifications)
        else:            
            d = self.factory.deferred
            timeout = reactor.callLater(self.timeout, 
                lambda: d.called or d.errback(
                    Exception('Notification timed out after %i seconds' % self.timeout)))
            def cancel_timeout(r):
                try: timeout.cancel()
                except: pass
                return r
            
            d.addCallback(lambda p: p.sendMessage(notifications))
            d.addErrback(log_errback('apns-service-write'))
            d.addBoth(cancel_timeout)
            return d
    
    def read(self):
        "Connect to the feedback service and read all data."
        log.msg('APNSService read (connecting)')
        try:
            server, port = ((FEEDBACK_SERVER_SANDBOX_HOSTNAME 
                                            if self.environment == 'sandbox'
                                            else FEEDBACK_SERVER_HOSTNAME), FEEDBACK_SERVER_PORT)
            factory = self.feedbackProtocolFactory()
            context = self.getContextFactory()
            reactor.connectSSL(server, port, factory, context)
            factory.deferred.addErrback(log_errback('apns-feedback-read'))
            
            timeout = reactor.callLater(self.timeout,
                lambda: factory.deferred.called or factory.deferred.errback(
                    Exception('Feedbcak fetch timed out after %i seconds' % self.timeout)))
            def cancel_timeout(r):
                try: timeout.cancel()
                except: pass
                return r
            
            factory.deferred.addBoth(cancel_timeout)
        except Exception, e:
            log.err('APNService feedback error initializing: %s' % str(e))
            raise
        return factory.deferred