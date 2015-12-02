unit CouchDB;

interface

uses jsonDoc, simpleSock;

type
  TCouchDBVerb=(vGET,vPUT,vPOST,vDELETE);
  TCouchDBConnection=class(TObject)
  private
    FSocket:TTcpSocket;
    FHostName:string;
    FPort:word;
  public
    constructor Create(const HostName:string='localhost';
      Port:word=5984);
    destructor Destroy; override;
    function Query(Verb:TCouchDBVerb;const QueryString:string;
      Data:IJSONDocument=nil):IJSONDocument;
  end;

implementation

uses SysUtils;

const
  CouchDBVerbs:array[TCouchDBVerb] of string=
    ('GET','PUT','POST','DELETE');

{ TCouchDBConnection }

constructor TCouchDBConnection.Create(
  const HostName:string='localhost';Port:word=5984);
begin
  inherited Create;
  //TODO: IPv6
  FSocket:=TTcpSocket.Create(AF_INET);
  FHostName:=HostName;
  FPort:=Port;
end;

destructor TCouchDBConnection.Destroy;
begin
  FSocket.Free;
  inherited;
end;

function TCouchDBConnection.Query(Verb:TCouchDBVerb;
  const QueryString:string;Data:IJSONDocument=nil):IJSONDocument;
var
  s:AnsiString;
  i,j,k,l,m,n,StatusNr:integer;
const
  recvBufSize=$4000;
begin
  //TODO: fail-over list of hostnames, reconnect
  if not FSocket.Connected then
    FSocket.Connect(FHostName,FPort);

  //TODO: protect/check QueryString!
  if Data=nil then
    s:=CouchDBVerbs[Verb]+' /'+QueryString+' HTTP/1.1'#13#10#13#10
  else
   begin
    s:=UTF8Encode(Data.ToString);
    s:=CouchDBVerbs[Verb]+' /'+QueryString+' HTTP/1.1'#13#10+
      'Content-Length: '+IntToStr(Length(s))+#13#10#13#10+s;
   end;
  //TODO: 'Host: '+FHostName+':'+IntToStr(FPort)+#13#10 ?

  l:=Length(s);
  i:=0;
  while (i<l) do
   begin
    j:=FSocket.SendBuf(s[i+1],l-i);
    if j=0 then raise Exception.Create('Transfer error');
    inc(i,j);
   end;

  SetLength(s,recvBufSize);
  l:=FSocket.ReceiveBuf(s[1],recvBufSize);
  if (l<12) or (Copy(s,1,9)<>'HTTP/1.1 ') then
    raise Exception.Create('Invalid response');
  StatusNr:=(byte(s[10]) and $F)*100
           +(byte(s[11]) and $F)*10
           +(byte(s[12]) and $F);
  j:=14;
  k:=0;
  while (j<=l) and (s[j]>=' ') do inc(j);
  repeat
    if j=l then
     begin
      n:=FSocket.ReceiveBuf(s[l],recvBufSize-l);
      if n=0 then raise Exception.Create('Transfer error');
      inc(l,n);
     end
    else
     begin
      if (j<=l) and (s[j]<' ') then inc(j);//CR
      if (j<=l) and (s[j]<' ') then inc(j);//LF
      i:=j;
     end;
    while (j<=l) and (s[j]>=' ') do inc(j);
    if (j-i>16) and (Copy(s,i,16)='Content-Length: ') then
     begin
      m:=i+16;
      while (m<j) do
       begin
        k:=k*10+(byte(s[m]) and $F);
        inc(m);
       end;
     end;
    //ignore the rest (?)
  until j=i;
  if (i<=l) and (s[i]<' ') then inc(i);//CR
  if (i<=l) and (s[i]<' ') then inc(i);//LF

  if k=0 then
    raise Exception.Create('Unable to obtain response size');

  if (l<i+k-1) then
   begin
    if i+k-1>recvBufSize then SetLength(s,i+k-1);
    while (l<i+k-1) do
     begin
      j:=FSocket.ReceiveBuf(s[l+1],i+k-l-1);
      if j=0 then raise Exception.Create('Transfer error');
      inc(l,j);
     end;
   end;

  case StatusNr of
    200:;//OK! assert Verb=vGet
    201:;//OK! assert Verb=vPut
    else
     begin
      j:=14;
      while (j<=l) and (s[j]>=' ') do inc(j);
      raise Exception.Create(IntToStr(StatusNr)+' '+Copy(s,14,j-14));
      //Result['reason']
     end;
  end;

  Result:=JSON;
  Result.Parse(UTF8Decode(Copy(s,i,k)));
end;

end.
