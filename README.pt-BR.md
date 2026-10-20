# Go Raft Distributed Consensus Key-Value Store (Português)

[![CI](https://github.com/FelipeMadson/go-raft-distributed-consensus-kv/actions/workflows/ci.yml/badge.svg)](https://github.com/FelipeMadson/go-raft-distributed-consensus-kv/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Go Version](https://img.shields.io/badge/Go-1.22+-00ADD8.svg)](https://golang.org)

Motor de consenso distribuído e armazenamento Chave-Valor tolerante a falhas construído do zero em Go 1.22+, implementando o **Protocolo de Consenso Distribuído Raft** (Ongaro & Ousterhout).

Projetado para execução sem dependências externas, linearizabilidade estrita, prevenção automática de *split-brain* e recuperação resiliente sob partições severas de rede.

---

## Visão Geral da Arquitetura

```
                      +-----------------------------+
                      |      Aplicação Cliente      |
                      +-----------------------------+
                                     |
                                (HTTP / RPC)
                                     v
                       +---------------------------+
                       |   Nó Líder (Raft KV)      |
                       |  - Propõe Entrada no Log  |
                       |  - Aguarda Quórum (N/2+1) |
                       +---------------------------+
                        /                         \
            AppendEntries (Termo T)     AppendEntries (Termo T)
                      /                             \
                     v                               v
       +---------------------------+   +---------------------------+
       |   Nó Seguidor (Peer 1)    |   |   Nó Seguidor (Peer 2)    |
       |  - Replica Log            |   |  - Replica Log            |
       |  - Comita & Aplica        |   |  - Comita & Aplica        |
       +---------------------------+   +---------------------------+
```

### Garantias Essenciais do Raft Implementadas

1. **Segurança de Eleição**: No máximo um líder pode ser eleito em determinado termo.
2. **Log Append-Only do Líder**: Um líder nunca sobrescreve ou trunca suas próprias entradas; apenas adiciona novas entradas.
3. **Propriedade de Correspondência de Log**: Se dois logs contêm uma entrada com o mesmo índice e termo, ambos são idênticos em todas as entradas até aquele índice.
4. **Completude do Líder**: Se uma entrada de log foi confirmada (comitada) em um termo, essa entrada estará presente nos logs dos líderes de todos os termos subsequentes.
5. **Segurança da Máquina de Estados**: Se um servidor aplicou uma entrada de log em determinado índice à sua máquina de estados, nenhum outro servidor aplicará uma entrada diferente para aquele mesmo índice.

---

## Funcionalidades Técnicas

- **Timeouts Aleatórios de Eleição**: Intervalos dinâmicos entre 150ms e 300ms impedem empates ou livelocks em eleições concorrentes.
- **Loop de Heartbeat e Replicação**: Transmissões contínuas de RPC `AppendEntries` com reconciliação imediata.
- **Recuperação Rápida de Conflito**: Backtracking veloz via `ConflictIndex` e `ConflictTerm` durante a cura de partições.
- **Leituras Linearizáveis e Deduplicação**: Mutações incorporam identificador de cliente (`ClientID`) e sequência monotônica (`SeqNum`), assegurando idempotência estrita sob repetições de rede.

---

## Estrutura de Diretórios

```
├── .github/workflows/
│   └── ci.yml                 # Pipeline CI com detector de condições de corrida (race detector)
├── cmd/
│   └── server/
│       └── main.go            # Cluster in-process e gateway HTTP
├── deploy/
│   └── prometheus.yml         # Configuração de métricas do Prometheus
├── docs/
│   └── adr/
│       ├── ADR-001-raft-consensus-protocol-selection.md
│       └── ADR-002-linearizable-read-lease-strategy.md
├── pkg/
│   ├── kv/
│   │   └── kv.go              # Máquina de estados KV e registro de deduplicação
│   ├── raft/
│   │   ├── raft.go            # Máquina de estados Raft (Follower/Candidate/Leader)
│   │   ├── rpc.go             # Definições de RequestVote e AppendEntries
│   │   └── storage.go         # Mecanismo de persistência de estado estável
│   └── transport/
│       └── network.go         # Simulação de rede RPC e controle de partições
├── tests/
│   ├── election_test.go       # Testes de eleição e failover
│   ├── kv_test.go             # Testes de operações KV e deduplicação
│   ├── partition_test.go      # Testes de partição de rede maioria vs minoria
│   └── replication_test.go    # Testes de replicação e quórum
├── docker-compose.yml         # Orquestração de cluster multi-nós em contêineres
├── Dockerfile                 # Build multi-estágio em Alpine (zero cgo)
├── go.mod                     # Definição do módulo Go 1.22
└── LICENSE                    # Licença MIT
```

---

## Execução Rápida

### Executando a Suíte de Testes
```bash
go test -v -race ./...
```

### Inicializando o Cluster Localmente
```bash
go run ./cmd/server
```

O cluster inicializará 3 nós Raft e exporá a porta HTTP `8080`.

#### Inserindo uma Chave Replicada:
```bash
curl -X POST http://localhost:8080/put \
  -H "Content-Type: application/json" \
  -d '{"key": "cluster.version", "value": "2026.4.1"}'
```

#### Leitura Linearizável:
```bash
curl "http://localhost:8080/get?key=cluster.version"
```

#### Diagnóstico e Topologia do Cluster:
```bash
curl http://localhost:8080/cluster
```

---

## Registros de Decisão de Arquitetura (ADRs)

- [ADR-001: Seleção do Protocolo de Consenso Raft](docs/adr/ADR-001-raft-consensus-protocol-selection.md)
- [ADR-002: Estratégia de Leituras Linearizáveis e Deduplicação](docs/adr/ADR-002-linearizable-read-lease-strategy.md)

---

## Autor

**Felipe Madison**  
Portfólio: [felipemadson.github.io/portfolio](https://felipemadson.github.io/portfolio/)  
GitHub: [@FelipeMadson](https://github.com/FelipeMadson)
