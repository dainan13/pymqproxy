#!/usr/bin/env python3
"""
a proxy for rocketmq
"""

__author__ = "dainan13"
__version__ = "1.0.0"

import asyncio
import struct
import json
import random
import time
import ipaddress
import fnmatch
import traceback

import aiosocks

import ffjson


def parse_addr( s ):
    
    _ip, _port = s.strip().strip().split(':',1)
    _ip = _ip.strip().strip()
    _ip = '0.0.0.0' if _ip == '' else _ip
    
    _port = int(_port)
    
    return (_ip, _port)
    
def match_subnet( _ip ):
    
    _ip = ipaddress.ip_network(_ip)
    
    def _match(s):
        try :
            return ipaddress.ip_address(s) in _ip
        except :
            pass
        return False
    
    return _match
    
def match_fnmatch( _ip ):
    
    def _match(s):
        return fnmatch.fnmatch( s, _ip )
    
    return _match
    
def match_true( ):
    
    def _match(s):
        return True
    
    return _match
    
def match_intport( _port ):
    
    _port = int(_port)
    
    def _match(s):
        try :
            return int(s) == _port
        except :
            pass
        return False
    
    return _match
    
def match_rangeport( _port ):
    
    _port_st, _port_ed = _port.split('-')
    _port_st, _port_ed = int(_port_st), int(_port_ed)
    _port_st, _port_ed = min(_port_st, _port_ed), int()
    
    def _match(s):
        try :
            s = int(s)
            return s >= _port_st and s <= _port_ed
        except :
            pass
        return False
        
    return _match
    
def parse_match( s ):
    
    if ':' in s :
        _ip, _port = s.strip().strip().split(':',1)
        _ip = _ip.strip()
        _port = _port.strip()
    else :
        _ip = s
        _port = None
    
    if '/' in _ip :
        mip = match_subnet(_ip)
    else :
        mip = match_fnmatch(_ip)
    
    if _port is None or _port == '*' :
        mport = match_true()
    elif '-' in _port :
        mport = match_rangeport(_port)
    else :
        mport = match_intport(_port)
    
    return lambda ip, port: mip(ip) and mport(port)
    
def parse_portgroup( s ):
    
    _ip, _port = s.strip().strip().split(':',1)
    _ip = _ip.strip().strip()
    _ip = '0.0.0.0' if _ip == '' else _ip
    
    _port_st, _port_ed, = _port.split('-',1)
    _port_st = int(_port_st)
    _port_ed = int(_port_ed)
    
    # 左小右大
    return (_ip, min(_port_st, _port_ed), max(_port_st, _port_ed) )

class ProxyServer(object):
    
    # 地址转换表
    proxytable = {}
    # 动态匹配的地址转换表
    dynamic_proxy = []
    
    # 记录启动的监听服务
    servers = []
    
    def __init__( self, stype, listenaddr, serveraddr, socksaddr ):
        
        self.listen_addr = parse_addr(listenaddr)
        self.server_addr = parse_addr(serveraddr)
        
        self.socks_addr = aiosocks.Socks5Addr( *parse_addr(socksaddr) ) if socksaddr else None
        
        return
        
    async def handle_client( self, client_reader, client_writer ):
        '''
        当客户端连接上来时生成代理管道
        '''
        
        dp = None
        
        try:
            dp = DuplexPipe( client_reader, client_writer )
            
            await dp.connect( self.server_addr, self.socks_addr )
            
            await asyncio.gather(dp.cs_pipe(), dp.sc_pipe())
            
        except Exception as e:
            traceback.print_exc()
            print('-------------', e)
            
        finally:
            if dp :
                dp.disconnect()
            
        return
        
    @classmethod
    async def find_proxy( cls, serveraddr, client_local_addr ):
        '''
        查找一个地址的替换地址
        如果找不到，则返回原始地址
        '''
        
        server_addr = parse_addr(serveraddr)
        proxy_addr = cls.proxytable.get(server_addr, None)
        
        if proxy_addr :
            if proxy_addr[0] == '0.0.0.0' :
                return '%s:%s' % (client_local_addr[0], proxy_addr[1])
            else :
                return '%s:%s' % proxy_addr
            
        for matchaddr, (listen_ip, portst, ported), passaddr in cls.dynamic_proxy :
            if matchaddr(*server_addr) :
                break
        else :
            return serveraddr
        
        # 随机找端口进行监听，如果被占用就重试，最高下面range次
        # 这里先进行sock的bind操作来找端口比较好，但不知道怎么跟asyncio一起用，后续改进
        for i in range(10):
            
            try :
                listen_port = random.randint(portst, ported)
                listen_addr = ( ( client_local_addr[0] if listen_ip=='0.0.0.0' else listen_ip ), listen_port )
                listenaddr = "%s:%s" % listen_addr
                s = cls("bk", listenaddr, serveraddr, passaddr)
                s.server = await asyncio.start_server(s.handle_client, s.listen_addr[0], s.listen_addr[1])
                cls.servers.append(s)
                cls.proxytable[server_addr] = listen_addr
            except AssertionError:
                continue
            
            return listenaddr
            
        return serveraddr
        
    @classmethod
    async def regist( cls, stype, listenaddr, socksaddr, remoteaddr ):
        '''
        注册一个监听服务
        '''
        
        socksaddr = None if socksaddr == '-' else socksaddr
        
        if stype == 'bk*' :
            
            cls.dynamic_proxy.append( (parse_match(remoteaddr), parse_portgroup(listenaddr), socksaddr) )
            
            listenaddr = None
            
        elif stype in ( 'bk', 'bkz' ) :
            
            cls.proxytable[parse_addr(remoteaddr)] = parse_addr( listenaddr )
        
        elif stype == 'ns':
            
            pass
        
        else :
            
            assert False, 'stype error %s' % stype
        
        if stype in ('ns', 'bk') :
            
            s = cls(stype, listenaddr, remoteaddr, socksaddr )
            s.server = await asyncio.start_server(s.handle_client, s.listen_addr[0], s.listen_addr[1])
            cls.servers.append( s ) 
            
            print('listen %s %s:%s' % (stype, *s.listen_addr) )
            
        return
    

class PrintLog(object):
    
    def __init__( self ):
        
        return
    
    def link_log( self, dp ):
        
        print( f'{dp.client_remote_addr[0]}:{dp.client_remote_addr[1]} <=[{dp.client_local_addr[0]}:{dp.client_local_addr[1]}]=> {dp.server_remote_addr[0]}:{dp.server_remote_addr[1]} |', 'connected' )
        
        return
    
    def unlink_log( self, dp ):
        
        if dp.server_reader :
            print( f'{dp.client_remote_addr[0]}:{dp.client_remote_addr[1]} ++[{dp.client_local_addr[0]}:{dp.client_local_addr[1]}]++ {dp.server_remote_addr[0]}:{dp.server_remote_addr[1]} |', 'disconnect' )
        elif dp.server_remote_addr :
            print( f'{dp.client_remote_addr[0]}:{dp.client_remote_addr[1]} ++[{dp.client_local_addr[0]}:{dp.client_local_addr[1]}]++ {dp.server_remote_addr[0]}:{dp.server_remote_addr[1]} |', 'can not connect' )
        else :
            print( f'{dp.client_remote_addr[0]}:{dp.client_remote_addr[1]} ++[{dp.client_local_addr[0]}:{dp.client_local_addr[1]}]++ ########### |', 'create error' )
        
        return
    
    def recv_cs_log( self, dp, head, content ):
        
        prefix = f'{dp.client_remote_addr[0]}:{dp.client_remote_addr[1]} --[{dp.client_local_addr[0]}:{dp.client_local_addr[1]}]-> {dp.server_remote_addr[0]}:{dp.server_remote_addr[1]} |'
        
        print( prefix, head )
        print( 'content |'.rjust(len(prefix)), content )
        
        return
    
    def recv_sc_log( self, dp, head, content ):
        
        prefix = f'{dp.client_remote_addr[0]}:{dp.client_remote_addr[1]} <-[{dp.client_local_addr[0]}:{dp.client_local_addr[1]}]-- {dp.server_remote_addr[0]}:{dp.server_remote_addr[1]} |'
        
        print( prefix, head )
        print( 'content |'.rjust(len(prefix)), content )
        
        return

    def replace_sc_log( self, dp, head, content ):
        
        prefix = f'{dp.client_remote_addr[0]}:{dp.client_remote_addr[1]} <-[{dp.client_local_addr[0]}:{dp.client_local_addr[1]}]-- {dp.server_remote_addr[0]}:{dp.server_remote_addr[1]} |'
        
        print( 'replace |'.rjust(len(prefix)), content )
        
        return
    
    def end_cs_log( self, dp ):
        
        print()
        
        return
        
    def end_sc_log( self, dp ):
        
        print()
        
        return
        
class DuplexPipe(object):
    
    log = PrintLog()
    
    #def __init__( self, proxyserver, client_reader, client_writer ):
    def __init__( self, client_reader, client_writer ):
        
        #client_remote_addr, client_local_addr, logprefix ):
        #self.proxy = proxyserver
        
        self.client_reader = client_reader
        self.client_writer = client_writer
        
        self.client_remote_addr = client_reader._transport.get_extra_info('peername')
        self.client_local_addr = client_reader._transport.get_extra_info('sockname')
        
        self.server_reader = None
        self.server_writer = None
        self.server_remote_addr = ('','')
        
        self.opaquedict = {}
        
        return
    
    async def connect( self, server_addr, socks_addr ):
        '''
        链接server端管道
        '''
        
        self.server_remote_addr = server_addr
        
        # 是否使用socks5代理
        if socks_addr is None :
            server_reader, server_writer = await asyncio.open_connection(*server_addr)
        else :
            server_reader, server_writer = await aiosocks.open_connection(
                proxy = socks_addr,
                proxy_auth = None,
                dst=server_addr,
                #remote_resolve = False
            )
        
        self.server_reader = server_reader
        self.server_writer = server_writer
        
        self.server_local_addr = server_reader._transport.get_extra_info('sockname')
        
        self.log.link_log(self)
        
        return
        
    def disconnect( self ):
        '''
        关闭所有管道
        '''
        
        try :
            self.client_reader.close()
        except :
            pass
            
        try :
            self.client_writer.close()
        except :
            pass
            
        try :
            self.server_reader.close()
        except :
            pass
        
        try :
            self.server_writer.close()
        except :
            pass
            
        self.log.unlink_log(self)
        
        return 
        
    async def cs_pipe( self ):
        '''
        客户端向服务器端通信的管道
        '''
        
        reader, writer = self.client_reader, self.server_writer
        
        buffer = b""
        
        while not reader.at_eof():
            
            data = await reader.read(1024*10)
            buffer = buffer + data
            
            if len(buffer) >= 8 :
                
                le, lehd = struct.unpack(">LL",buffer[:8])
                
                if len(buffer) >= 4+le :
                    
                    header = ffjson.loads( buffer[8:8+lehd].decode('utf-8') )
                    content = buffer[8+lehd:4+le]
                    
                    self.log.recv_cs_log(self, header, content)
                    
                    buffer = buffer[4+le:]
                    
                    # 记录请求的opaque码,用于另一管道知道是那个指令码的回执
                    if 'opaque' in header :
                        self.opaquedict[header['opaque']] = header
                    
                    b_header = ffjson.dumps(header).encode('utf-8')
                    b_content = content
                    
                    backdata = struct.pack( ">LL", 
                            4+len(b_header)+len(b_content),
                            len(b_header),
                    ) +b_header+b_content
                    
                    self.log.end_cs_log(self)
                    
                    writer.write( backdata )
        
        return
    
    async def sc_pipe( self ):
        '''
        服务器端向客户端通信的管道
        '''
        
        reader, writer = self.server_reader, self.client_writer
        
        buffer = b""
        
        while not reader.at_eof():
            
            data = await reader.read(1024*10)
            buffer = buffer + data
            
            if len(buffer) >= 8 :
                
                le, lehd = struct.unpack(">LL",buffer[:8])
                
                if len(buffer) >= 4+le :
                    
                    header = ffjson.loads( buffer[8:8+lehd].decode('utf-8') )
                    content = buffer[8+lehd:4+le]
                    
                    self.log.recv_sc_log(self, header, content)
                    
                    buffer = buffer[4+le:]
                    
                    # 通过opaque码找到客户端当时的调用指令记录
                    cmdheader = None
                    if 'opaque' in header :
                        cmdheader = self.opaquedict.pop(header['opaque'],None)
                    
                    # 查看调用指令是否是命令ns返回broker的地址的指令
                    if cmdheader and cmdheader['code'] == 105 and content:
                        
                        content = ffjson.loads(content.decode('utf-8'))
                        
                        if content and 'brokerDatas' in content :
                            
                            for broker_data in content['brokerDatas'] :
                                
                                for broker_key, broker_addr in broker_data['brokerAddrs'].items():
                                    
                                    # 将broker地址替换为本服务对应的代理地址
                                    broker_data['brokerAddrs'][broker_key] = await ProxyServer.find_proxy( broker_addr, self.client_local_addr )
                            
                        content = ffjson.dumps(content).encode('utf-8')
                    
                        self.log.replace_sc_log(self, header, content)
                    
                    b_header = ffjson.dumps(header).encode('utf-8')
                    b_content = content
                    
                    backdata = struct.pack( ">LL", 
                            4+len(b_header)+len(b_content),
                            len(b_header),
                    ) +b_header+b_content
                    
                    self.log.end_sc_log(self)
                    
                    writer.write( backdata )
        
        return


if __name__ == '__main__' :
    
    #with open('/etc/pymqproxy/env') as fp :
    #    pass
    
    with open('/etc/pymqproxy/proxy') as fp :
        
        cnt = fp.read()
        config = [ conf.strip() for conf in cnt.splitlines() ]
        config = [ conf.split() for conf in config if conf and (not conf.startswith('#')) ]
        
        for conf in config :
            assert len(conf) == 4, conf
            assert conf[0] in ('bk','bk*','bkz','ns'), conf
                
    loop = asyncio.get_event_loop()
    loop.run_until_complete( asyncio.gather(*[ ProxyServer.regist(*conf) for conf in config ]) )

    # Serve requests until Ctrl+C is pressed
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
        
    for s in ProxyServer.servers:
        s.server.close()
        
    loop.run_until_complete( asyncio.gather(*[s.server.wait_closed() for s in ProxyServer.servers ] ) )
    loop.close()
    
    print()




