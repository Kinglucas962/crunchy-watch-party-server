# Crunchy Watch Party Server

Servidor WebSocket da extensão Crunchy Watch Party.

## Executar

```bash
npm install
npm test
npm start
```

O servidor usa a porta definida em `PORT` ou `8080` por padrão.

Em produção, defina `ALLOWED_ORIGINS` com as origens autorizadas, separadas por vírgula. Para uma extensão Chrome, use `chrome-extension://ID_DA_EXTENSAO`. Se a variável não for configurada, o servidor mantém compatibilidade com clientes existentes e aceita qualquer origem.

## Reconexão segura

Ao entrar pela primeira vez, `ROOM_JOINED` inclui um `sessionToken`. A extensão deve salvar esse token em `chrome.storage.local` e enviá-lo nos próximos `JOIN_ROOM` do mesmo `roomId` e `clientId`.

```json
{
  "type": "JOIN_ROOM",
  "roomId": "SALA1234",
  "clientId": "cliente-uuid",
  "sessionToken": "token-recebido-anteriormente",
  "createRoom": false
}
```

O token não deve ser exibido no painel, enviado pelo chat ou compartilhado com outros participantes.

## Limites de segurança

- Payload WebSocket máximo: 512 KB.
- Foto personalizada máxima: 100.000 caracteres em PNG, JPEG, WebP ou GIF Base64.
- Estado do player máximo: 8 KB, somente com valores simples.
- URLs de navegação: somente HTTPS no domínio `crunchyroll.com`.
- Chat, reações e comandos do player possuem limitação de frequência.

## Testes

Os testes verificam URLs permitidas, validação do estado do player, proteção do anfitrião, autenticação de reconexão e ausência de fotos Base64 duplicadas nas mensagens.
