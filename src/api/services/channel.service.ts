import { InstanceDto } from '@api/dto/instance.dto';
import { ProxyDto } from '@api/dto/proxy.dto';
import { SettingsDto } from '@api/dto/settings.dto';
import { ChatwootDto } from '@api/integrations/chatbot/chatwoot/dto/chatwoot.dto';
import { ChatwootService } from '@api/integrations/chatbot/chatwoot/services/chatwoot.service';
import { DifyService } from '@api/integrations/chatbot/dify/services/dify.service';
import { OpenaiService } from '@api/integrations/chatbot/openai/services/openai.service';
import { TypebotService } from '@api/integrations/chatbot/typebot/services/typebot.service';
import { PrismaRepository, Query } from '@api/repository/repository.service';
import { eventManager, waMonitor } from '@api/server.module';
import { Events, wa } from '@api/types/wa.types';
import { Auth, Chatwoot, ConfigService, HttpServer, Proxy } from '@config/env.config';
import { Logger } from '@config/logger.config';
import { NotFoundException } from '@exceptions';
import { Contact, Message, Prisma } from '@prisma/client';
import { createJid } from '@utils/createJid';
import { WASocket } from 'baileys';
import { isArray } from 'class-validator';
import EventEmitter2 from 'eventemitter2';
import { v4 } from 'uuid';

import { CacheService } from './cache.service';

export class ChannelStartupService {
  constructor(
    public readonly configService: ConfigService,
    public readonly eventEmitter: EventEmitter2,
    public readonly prismaRepository: PrismaRepository,
    public readonly chatwootCache: CacheService,
  ) {}

  public readonly logger = new Logger('ChannelStartupService');

  public client: WASocket;
  public readonly instance: wa.Instance = {};
  public readonly localChatwoot: wa.LocalChatwoot = {};
  public readonly localProxy: wa.LocalProxy = {};
  public readonly localSettings: wa.LocalSettings = {};
  public readonly localWebhook: wa.LocalWebHook = {};

  public chatwootService = new ChatwootService(
    waMonitor,
    this.configService,
    this.prismaRepository,
    this.chatwootCache,
  );

  public openaiService = new OpenaiService(waMonitor, this.prismaRepository, this.configService);

  public typebotService = new TypebotService(waMonitor, this.configService, this.prismaRepository, this.openaiService);

  public difyService = new DifyService(waMonitor, this.prismaRepository, this.configService, this.openaiService);

  public setInstance(instance: InstanceDto) {
    this.logger.setInstance(instance.instanceName);

    this.instance.name = instance.instanceName;
    this.instance.id = instance.instanceId;
    this.instance.integration = instance.integration;
    this.instance.number = instance.number;
    this.instance.token = instance.token;
    this.instance.businessId = instance.businessId;

    if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED && this.localChatwoot?.enabled) {
      this.chatwootService.eventWhatsapp(
        Events.STATUS_INSTANCE,
        { instanceName: this.instance.name },
        {
          instance: this.instance.name,
          status: 'created',
        },
      );
    }
  }

  public set instanceName(name: string) {
    this.logger.setInstance(name);

    if (!name) {
      this.instance.name = v4();
      return;
    }
    this.instance.name = name;
  }

  public get instanceName() {
    return this.instance.name;
  }

  public set instanceId(id: string) {
    if (!id) {
      this.instance.id = v4();
      return;
    }
    this.instance.id = id;
  }

  public get instanceId() {
    return this.instance.id;
  }

  public set integration(integration: string) {
    this.instance.integration = integration;
  }

  public get integration() {
    return this.instance.integration;
  }

  public set number(number: string) {
    this.instance.number = number;
  }

  public get number() {
    return this.instance.number;
  }

  public set token(token: string) {
    this.instance.token = token;
  }

  public get token() {
    return this.instance.token;
  }

  public get wuid() {
    return this.instance.wuid;
  }

  public async loadWebhook() {
    const data = await this.prismaRepository.webhook.findUnique({
      where: {
        instanceId: this.instanceId,
      },
    });

    this.localWebhook.enabled = data?.enabled;
    this.localWebhook.webhookBase64 = data?.webhookBase64;
  }

  public async loadSettings() {
    const data = await this.prismaRepository.setting.findUnique({
      where: {
        instanceId: this.instanceId,
      },
    });

    this.localSettings.rejectCall = data?.rejectCall;
    this.localSettings.msgCall = data?.msgCall;
    this.localSettings.groupsIgnore = data?.groupsIgnore;
    this.localSettings.alwaysOnline = data?.alwaysOnline;
    this.localSettings.readMessages = data?.readMessages;
    this.localSettings.readStatus = data?.readStatus;
    this.localSettings.syncFullHistory = data?.syncFullHistory;
    this.localSettings.wavoipToken = data?.wavoipToken;
  }

  public async setSettings(data: SettingsDto) {
    await this.prismaRepository.setting.upsert({
      where: {
        instanceId: this.instanceId,
      },
      update: {
        rejectCall: data.rejectCall,
        msgCall: data.msgCall,
        groupsIgnore: data.groupsIgnore,
        alwaysOnline: data.alwaysOnline,
        readMessages: data.readMessages,
        readStatus: data.readStatus,
        syncFullHistory: data.syncFullHistory,
        wavoipToken: data.wavoipToken,
      },
      create: {
        rejectCall: data.rejectCall,
        msgCall: data.msgCall,
        groupsIgnore: data.groupsIgnore,
        alwaysOnline: data.alwaysOnline,
        readMessages: data.readMessages,
        readStatus: data.readStatus,
        syncFullHistory: data.syncFullHistory,
        wavoipToken: data.wavoipToken,
        instanceId: this.instanceId,
      },
    });

    this.localSettings.rejectCall = data?.rejectCall;
    this.localSettings.msgCall = data?.msgCall;
    this.localSettings.groupsIgnore = data?.groupsIgnore;
    this.localSettings.alwaysOnline = data?.alwaysOnline;
    this.localSettings.readMessages = data?.readMessages;
    this.localSettings.readStatus = data?.readStatus;
    this.localSettings.syncFullHistory = data?.syncFullHistory;
    this.localSettings.wavoipToken = data?.wavoipToken;

    if (this.localSettings.wavoipToken && this.localSettings.wavoipToken.length > 0) {
      this.client.ws.close();
      this.client.ws.connect();
    }
  }

  public async findSettings() {
    const data = await this.prismaRepository.setting.findUnique({
      where: {
        instanceId: this.instanceId,
      },
    });

    if (!data) {
      return null;
    }

    return {
      rejectCall: data.rejectCall,
      msgCall: data.msgCall,
      groupsIgnore: data.groupsIgnore,
      alwaysOnline: data.alwaysOnline,
      readMessages: data.readMessages,
      readStatus: data.readStatus,
      syncFullHistory: data.syncFullHistory,
      wavoipToken: data.wavoipToken,
    };
  }

  public async loadChatwoot() {
    if (!this.configService.get<Chatwoot>('CHATWOOT').ENABLED) {
      return;
    }

    const data = await this.prismaRepository.chatwoot.findUnique({
      where: {
        instanceId: this.instanceId,
      },
    });

    this.localChatwoot.enabled = data?.enabled;
    this.localChatwoot.accountId = data?.accountId;
    this.localChatwoot.token = data?.token;
    this.localChatwoot.url = data?.url;
    this.localChatwoot.nameInbox = data?.nameInbox;
    this.localChatwoot.signMsg = data?.signMsg;
    this.localChatwoot.signDelimiter = data?.signDelimiter;
    this.localChatwoot.number = data?.number;
    this.localChatwoot.reopenConversation = data?.reopenConversation;
    this.localChatwoot.conversationPending = data?.conversationPending;
    this.localChatwoot.mergeBrazilContacts = data?.mergeBrazilContacts;
    this.localChatwoot.importContacts = data?.importContacts;
    this.localChatwoot.importMessages = data?.importMessages;
    this.localChatwoot.daysLimitImportMessages = data?.daysLimitImportMessages;
  }

  public async setChatwoot(data: ChatwootDto) {
    if (!this.configService.get<Chatwoot>('CHATWOOT').ENABLED) {
      return;
    }

    const chatwoot = await this.prismaRepository.chatwoot.findUnique({
      where: {
        instanceId: this.instanceId,
      },
    });

    if (chatwoot) {
      await this.prismaRepository.chatwoot.update({
        where: {
          instanceId: this.instanceId,
        },
        data: {
          enabled: data?.enabled,
          accountId: data.accountId,
          token: data.token,
          url: data.url,
          nameInbox: data.nameInbox,
          signMsg: data.signMsg,
          signDelimiter: data.signMsg ? data.signDelimiter : null,
          number: data.number,
          reopenConversation: data.reopenConversation,
          conversationPending: data.conversationPending,
          mergeBrazilContacts: data.mergeBrazilContacts,
          importContacts: data.importContacts,
          importMessages: data.importMessages,
          daysLimitImportMessages: data.daysLimitImportMessages,
          organization: data.organization,
          logo: data.logo,
          ignoreJids: data.ignoreJids,
        },
      });

      Object.assign(this.localChatwoot, { ...data, signDelimiter: data.signMsg ? data.signDelimiter : null });

      this.clearCacheChatwoot();
      return;
    }

    await this.prismaRepository.chatwoot.create({
      data: {
        enabled: data?.enabled,
        accountId: data.accountId,
        token: data.token,
        url: data.url,
        nameInbox: data.nameInbox,
        signMsg: data.signMsg,
        number: data.number,
        reopenConversation: data.reopenConversation,
        conversationPending: data.conversationPending,
        mergeBrazilContacts: data.mergeBrazilContacts,
        importContacts: data.importContacts,
        importMessages: data.importMessages,
        daysLimitImportMessages: data.daysLimitImportMessages,
        organization: data.organization,
        logo: data.logo,
        ignoreJids: data.ignoreJids,
        instanceId: this.instanceId,
      },
    });

    Object.assign(this.localChatwoot, { ...data, signDelimiter: data.signMsg ? data.signDelimiter : null });

    this.clearCacheChatwoot();
  }

  public async findChatwoot(): Promise<ChatwootDto | null> {
    if (!this.configService.get<Chatwoot>('CHATWOOT').ENABLED) {
      return null;
    }

    const data = await this.prismaRepository.chatwoot.findUnique({
      where: {
        instanceId: this.instanceId,
      },
    });

    if (!data) {
      return null;
    }

    const ignoreJidsArray = Array.isArray(data.ignoreJids) ? data.ignoreJids.map((event) => String(event)) : [];

    return {
      enabled: data?.enabled,
      accountId: data.accountId,
      token: data.token,
      url: data.url,
      nameInbox: data.nameInbox,
      signMsg: data.signMsg,
      signDelimiter: data.signDelimiter || null,
      reopenConversation: data.reopenConversation,
      conversationPending: data.conversationPending,
      mergeBrazilContacts: data.mergeBrazilContacts,
      importContacts: data.importContacts,
      importMessages: data.importMessages,
      daysLimitImportMessages: data.daysLimitImportMessages,
      organization: data.organization,
      logo: data.logo,
      ignoreJids: ignoreJidsArray,
    };
  }

  public clearCacheChatwoot() {
    if (this.localChatwoot?.enabled) {
      this.chatwootService.getCache()?.deleteAll(this.instanceName);
    }
  }

  public async loadProxy() {
    this.localProxy.enabled = false;

    const proxyConfig = this.configService.get<Proxy>('PROXY');
    if (proxyConfig.HOST) {
      this.localProxy.enabled = true;
      this.localProxy.host = proxyConfig.HOST;
      this.localProxy.port = proxyConfig.PORT || '80';
      this.localProxy.protocol = proxyConfig.PROTOCOL || 'http';
      this.localProxy.username = proxyConfig.USERNAME;
      this.localProxy.password = proxyConfig.PASSWORD;
    }

    const data = await this.prismaRepository.proxy.findUnique({
      where: {
        instanceId: this.instanceId,
      },
    });

    if (data?.enabled) {
      this.localProxy.enabled = true;
      this.localProxy.host = data?.host;
      this.localProxy.port = data?.port;
      this.localProxy.protocol = data?.protocol;
      this.localProxy.username = data?.username;
      this.localProxy.password = data?.password;
    }
  }

  public async setProxy(data: ProxyDto) {
    await this.prismaRepository.proxy.upsert({
      where: {
        instanceId: this.instanceId,
      },
      update: {
        enabled: data?.enabled,
        host: data.host,
        port: data.port,
        protocol: data.protocol,
        username: data.username,
        password: data.password,
      },
      create: {
        enabled: data?.enabled,
        host: data.host,
        port: data.port,
        protocol: data.protocol,
        username: data.username,
        password: data.password,
        instanceId: this.instanceId,
      },
    });

    Object.assign(this.localProxy, data);
  }

  public async findProxy() {
    const data = await this.prismaRepository.proxy.findUnique({
      where: {
        instanceId: this.instanceId,
      },
    });

    if (!data) {
      throw new NotFoundException('Proxy not found');
    }

    return data;
  }

  public async sendDataWebhook<T extends object = any>(event: Events, data: T, local = true, integration?: string[]) {
    const serverUrl = this.configService.get<HttpServer>('SERVER').URL;
    const tzoffset = new Date().getTimezoneOffset() * 60000; //offset in milliseconds
    const localISOTime = new Date(Date.now() - tzoffset).toISOString();
    const now = localISOTime;

    const expose = this.configService.get<Auth>('AUTHENTICATION').EXPOSE_IN_FETCH_INSTANCES;

    const instanceApikey = this.token || 'Apikey not found';

    await eventManager.emit({
      instanceName: this.instance.name,
      origin: ChannelStartupService.name,
      event,
      data,
      serverUrl,
      dateTime: now,
      sender: this.wuid,
      apiKey: expose && instanceApikey ? instanceApikey : null,
      local,
      integration,
    });
  }

  // Check if the number is MX or AR
  public formatMXOrARNumber(jid: string): string {
    const countryCode = jid.substring(0, 2);

    if (Number(countryCode) === 52 || Number(countryCode) === 54) {
      if (jid.length === 13) {
        const number = countryCode + jid.substring(3);
        return number;
      }

      return jid;
    }
    return jid;
  }

  // Check if the number is br
  public formatBRNumber(jid: string) {
    const regexp = new RegExp(/^(\d{2})(\d{2})\d{1}(\d{8})$/);
    if (regexp.test(jid)) {
      const match = regexp.exec(jid);
      if (match && match[1] === '55') {
        const joker = Number.parseInt(match[3][0]);
        const ddd = Number.parseInt(match[2]);
        if (joker < 7 || ddd < 31) {
          return match[0];
        }
        return match[1] + match[2] + match[3];
      }
      return jid;
    } else {
      return jid;
    }
  }

  public async fetchContacts(query: Query<Contact>) {
    const remoteJid = query?.where?.remoteJid
      ? query?.where?.remoteJid.includes('@')
        ? query.where?.remoteJid
        : createJid(query.where?.remoteJid)
      : null;

    const where = {
      instanceId: this.instanceId,
    };

    if (remoteJid) {
      where['remoteJid'] = remoteJid;
    }

    const contactFindManyArgs: Prisma.ContactFindManyArgs = {
      where,
    };

    if (query.offset) contactFindManyArgs.take = query.offset;
    if (query.page) {
      const validPage = Math.max(query.page as number, 1);
      contactFindManyArgs.skip = query.offset * (validPage - 1);
    }

    const contacts = await this.prismaRepository.contact.findMany(contactFindManyArgs);

    return contacts.map((contact) => {
      const remoteJid = contact.remoteJid;
      const isGroup = remoteJid.endsWith('@g.us');
      const isSaved = !!contact.pushName || !!contact.profilePicUrl;
      const type = isGroup ? 'group' : isSaved ? 'contact' : 'group_member';
      return {
        ...contact,
        isGroup,
        isSaved,
        type,
      };
    });
  }

  public cleanMessageData(message: any) {
    if (!message) return message;

    const cleanedMessage = { ...message };

    const mediaUrl = cleanedMessage.message.mediaUrl;

    delete cleanedMessage.message.base64;

    if (cleanedMessage.message) {
      // Limpa imageMessage
      if (cleanedMessage.message.imageMessage) {
        cleanedMessage.message.imageMessage = {
          caption: cleanedMessage.message.imageMessage.caption,
        };
      }

      // Limpa videoMessage
      if (cleanedMessage.message.videoMessage) {
        cleanedMessage.message.videoMessage = {
          caption: cleanedMessage.message.videoMessage.caption,
        };
      }

      // Limpa audioMessage
      if (cleanedMessage.message.audioMessage) {
        cleanedMessage.message.audioMessage = {
          seconds: cleanedMessage.message.audioMessage.seconds,
        };
      }

      // Limpa stickerMessage
      if (cleanedMessage.message.stickerMessage) {
        cleanedMessage.message.stickerMessage = {};
      }

      // Limpa documentMessage
      if (cleanedMessage.message.documentMessage) {
        cleanedMessage.message.documentMessage = {
          caption: cleanedMessage.message.documentMessage.caption,
          name: cleanedMessage.message.documentMessage.name,
        };
      }

      // Limpa documentWithCaptionMessage
      if (cleanedMessage.message.documentWithCaptionMessage) {
        cleanedMessage.message.documentWithCaptionMessage = {
          caption: cleanedMessage.message.documentWithCaptionMessage.caption,
          name: cleanedMessage.message.documentWithCaptionMessage.name,
        };
      }
    }

    if (mediaUrl) cleanedMessage.message.mediaUrl = mediaUrl;

    return cleanedMessage;
  }

  public async fetchMessages(query: Query<Message>) {
    const keyFilters = query?.where?.key as {
      id?: string;
      fromMe?: boolean;
      remoteJid?: string;
      participants?: string;
    };

    const timestampFilter = {};
    if (query?.where?.messageTimestamp) {
      if (query.where.messageTimestamp['gte'] && query.where.messageTimestamp['lte']) {
        timestampFilter['messageTimestamp'] = {
          gte: Math.floor(new Date(query.where.messageTimestamp['gte']).getTime() / 1000),
          lte: Math.floor(new Date(query.where.messageTimestamp['lte']).getTime() / 1000),
        };
      }
    }

    const count = await this.prismaRepository.message.count({
      where: {
        instanceId: this.instanceId,
        id: query?.where?.id,
        source: query?.where?.source,
        messageType: query?.where?.messageType,
        ...timestampFilter,
        AND: [
          keyFilters?.id ? { key: { path: ['id'], equals: keyFilters?.id } } : {},
          keyFilters?.fromMe ? { key: { path: ['fromMe'], equals: keyFilters?.fromMe } } : {},
          keyFilters?.remoteJid ? { key: { path: ['remoteJid'], equals: keyFilters?.remoteJid } } : {},
          keyFilters?.participants ? { key: { path: ['participants'], equals: keyFilters?.participants } } : {},
        ],
      },
    });

    if (!query?.offset) {
      query.offset = 50;
    }

    if (!query?.page) {
      query.page = 1;
    }

    const messages = await this.prismaRepository.message.findMany({
      where: {
        instanceId: this.instanceId,
        id: query?.where?.id,
        source: query?.where?.source,
        messageType: query?.where?.messageType,
        ...timestampFilter,
        AND: [
          keyFilters?.id ? { key: { path: ['id'], equals: keyFilters?.id } } : {},
          keyFilters?.fromMe ? { key: { path: ['fromMe'], equals: keyFilters?.fromMe } } : {},
          keyFilters?.remoteJid ? { key: { path: ['remoteJid'], equals: keyFilters?.remoteJid } } : {},
          keyFilters?.participants ? { key: { path: ['participants'], equals: keyFilters?.participants } } : {},
        ],
      },
      orderBy: {
        messageTimestamp: 'desc',
      },
      skip: query.offset * (query?.page === 1 ? 0 : (query?.page as number) - 1),
      take: query.offset,
      select: {
        id: true,
        key: true,
        pushName: true,
        messageType: true,
        message: true,
        messageTimestamp: true,
        instanceId: true,
        source: true,
        contextInfo: true,
        MessageUpdate: {
          select: {
            status: true,
          },
        },
      },
    });

    return {
      messages: {
        total: count,
        pages: Math.ceil(count / query.offset),
        currentPage: query.page,
        records: messages,
      },
    };
  }

  public async fetchVezoMessages(query: Query<Message>) {
    // -------- Paginação
    const page = Number.isInteger(query?.page) ? Math.max(0, Number(query.page)) : 0;
    const limit = Math.max(1, Number(query?.limit ?? 50) || 50);
    const skip = page * limit;

    // -------- Filtros
    const keyFilters = (query?.where as any)?.key ?? {};
    const timestampFilter: Prisma.Sql[] = [];

    if ((query?.where as any)?.messageTimestamp) {
      const mt = (query.where as any).messageTimestamp;
      if (mt.gte)
        timestampFilter.push(
          Prisma.sql`m."messageTimestamp" >= ${Math.floor(new Date(mt.gte).getTime() / 1000)}`
        );
      if (mt.lte)
        timestampFilter.push(
          Prisma.sql`m."messageTimestamp" <= ${Math.floor(new Date(mt.lte).getTime() / 1000)}`
        );
    }

    const messageTypeFilter =
      (query?.where as any)?.messageType && (query.where as any).messageType !== 'reactionMessage'
        ? Prisma.sql`m."messageType" = ${query.where.messageType}`
        : Prisma.sql`m."messageType" != 'reactionMessage'`;

    const jsonFilters: Prisma.Sql[] = [];

    if (keyFilters?.id)
      jsonFilters.push(Prisma.sql`(m."key"->>'id') = ${keyFilters.id}`);

    if (typeof keyFilters?.fromMe === 'boolean')
      jsonFilters.push(
        Prisma.sql`(m."key"->>'fromMe')::boolean = ${keyFilters.fromMe}`
      );

    // Aqui entra o NOVO: remoteJid OU remoteJidAlt
    if (keyFilters?.remoteJid)
      jsonFilters.push(
        Prisma.sql`
          (
            (m."key"->>'remoteJid') = ${keyFilters.remoteJid}
            OR
            (m."key"->>'remoteJidAlt') = ${keyFilters.remoteJid}
          )
        `
      );

    if (keyFilters?.participant)
      jsonFilters.push(
        Prisma.sql`(m."key"->>'participant') = ${keyFilters.participant}`
      );

    const whereClauses = [
      Prisma.sql`m."instanceId" = ${this.instanceId}`,
      messageTypeFilter,
      ...timestampFilter,
      ...jsonFilters,
    ];

    const whereSql = whereClauses.length
      ? Prisma.sql`WHERE ${Prisma.join(whereClauses, ' AND ')}`
      : Prisma.sql``;

    // -------- Mensagens deduplicadas + MessageUpdate
    const messages = await this.prismaRepository.$queryRaw<Array<any>>`
      SELECT
        m.*,
        COALESCE(
          JSON_AGG(
            DISTINCT JSONB_BUILD_OBJECT(
              'status', mu."status",
              'fromMe', mu."fromMe",
              'participant', mu."participant",
              'keyId', mu."keyId"
            )
          ) FILTER (WHERE mu."id" IS NOT NULL),
          '[]'
        ) AS "messageUpdates"
      FROM (
        SELECT DISTINCT ON (m."key"->>'id', m."instanceId")
          m."id",
          m."key",
          m."pushName",
          m."messageType",
          m."message",
          m."messageTimestamp",
          m."instanceId",
          m."source",
          m."contextInfo"
        FROM "Message" m
        ${whereSql}
        ORDER BY m."key"->>'id', m."instanceId", m."messageTimestamp" DESC
      ) AS m
      LEFT JOIN "MessageUpdate" mu ON mu."messageId" = m."id"
      GROUP BY
        m."id",
        m."key",
        m."pushName",
        m."messageType",
        m."message",
        m."messageTimestamp",
        m."instanceId",
        m."source",
        m."contextInfo"
      ORDER BY m."messageTimestamp" DESC, m."id" DESC
      LIMIT ${limit}
      OFFSET ${skip};
    `;

    // -------- Total de mensagens distintas
    const totalRows = await this.prismaRepository.$queryRaw<
      Array<{ total: number }>
    >`
      SELECT COUNT(*)::int AS total
      FROM (
        SELECT DISTINCT ON (m."key"->>'id', m."instanceId") 1
        FROM "Message" m
        ${whereSql}
        ORDER BY m."key"->>'id', m."instanceId", m."messageTimestamp" DESC
      ) AS distinct_msgs;
    `;
    const total = totalRows?.[0]?.total ?? 0;

    // -------- Alvos das reações
    type ParentTarget = { id: string; remoteJid?: string; remoteJidAlt?: string };
    const parentTargets: ParentTarget[] = messages
      .map((m) => {
        const k = m.key as any;
        const id = k?.id ? String(k.id) : undefined;
        if (!id) return null;
        return {
          id,
          remoteJid: k?.remoteJid ? String(k.remoteJid) : undefined,
          remoteJidAlt: k?.remoteJidAlt ? String(k.remoteJidAlt) : undefined,
        };
      })
      .filter(Boolean) as ParentTarget[];

    let reactionsByTarget = new Map<string, any[]>();

    if (parentTargets.length > 0) {
      const reactionWhereOR: Prisma.Sql[] = parentTargets.map((t) => {
        const ands: Prisma.Sql[] = [
          Prisma.sql`(m."message"->'reactionMessage'->'key'->>'id') = ${t.id}`,
        ];

        if (t.remoteJid)
          ands.push(
            Prisma.sql`(m."message"->'reactionMessage'->'key'->>'remoteJid') = ${t.remoteJid}`
          );

        // reaction NÃO possui remoteJidAlt → ignorado

        return Prisma.sql`(${Prisma.join(ands, ' AND ')})`;
      });

      const reactionWhere = Prisma.sql`
        WHERE m."instanceId" = ${this.instanceId}
          AND m."messageType" = 'reactionMessage'
          ${reactionWhereOR.length
            ? Prisma.sql`AND (${Prisma.join(reactionWhereOR, ' OR ')})`
            : Prisma.sql``}
      `;

      const reactions = await this.prismaRepository.$queryRaw<Array<any>>`
        SELECT *
        FROM (
          SELECT DISTINCT ON (
            m."message"->'reactionMessage'->'key'->>'remoteJid',
            m."message"->'reactionMessage'->'key'->>'id',
            (m."key"->>'fromMe')::boolean
          )
            m."id",
            m."key",
            m."message",
            m."messageTimestamp",
            m."pushName",
            m."source",
            m."instanceId",
            m."contextInfo"
          FROM "Message" m
          ${reactionWhere}
          ORDER BY
            m."message"->'reactionMessage'->'key'->>'remoteJid',
            m."message"->'reactionMessage'->'key'->>'id',
            (m."key"->>'fromMe')::boolean,
            m."messageTimestamp" DESC
        ) AS distinct_rx
        ORDER BY distinct_rx."messageTimestamp" DESC;
      `;

      reactionsByTarget = reactions.reduce((map, rx) => {
        const msg = rx.message as any;
        const tgt = msg?.reactionMessage?.key;
        const mapKey = `${tgt?.remoteJid ?? ''}::${tgt?.id ?? ''}`;

        const data = {
          id: rx.id,
          emoji: msg?.reactionMessage?.text ?? null,
          fromMe: (rx.key as any)?.fromMe ?? null,
          by: (rx.key as any)?.participant ?? null,
          remoteJid: tgt?.remoteJid ?? null,
          senderTimestampMs: msg?.reactionMessage?.senderTimestampMs ?? null,
          messageTimestamp: rx.messageTimestamp,
          pushName: rx.pushName ?? null,
          source: rx.source,
          contextInfo: rx.contextInfo ?? null,
        };

        const arr = map.get(mapKey) ?? [];
        arr.push(data);
        map.set(mapKey, arr);
        return map;
      }, new Map<string, any[]>());
    }

    // -------- Anexar reações
    const recordsWithReactions = messages.map((m) => {
      const k = m.key as any;
      const mapKey = `${k?.remoteJid ?? ''}::${k?.id ?? ''}`; // remoteJidAlt não existe nas reações!
      const reactions = reactionsByTarget.get(mapKey) ?? [];
      return { ...m, reactions };
    });

    // -------- Paginação
    const pages = total === 0 ? 0 : Math.ceil(total / limit);

    return {
      messages: {
        total,
        pages,
        currentPage: page,
        limit,
        records: recordsWithReactions,
      },
    };
  }



  public async fetchVezoChats(query: {
    where?: {
      remoteJid?: string;
      messageTimestamp?: { gte?: string | Date; lte?: string | Date };
    };
    take?: number;
    skip?: number;
  }) {
    // --- filtros e paginação (sem non-null assertion) ---
    const remoteJid =
      query?.where?.remoteJid
        ? (query.where.remoteJid.includes('@') ? query.where.remoteJid : createJid(query.where.remoteJid))
        : null;

    const gte =
      query?.where?.messageTimestamp?.gte != null
        ? Math.floor(new Date(query.where!.messageTimestamp!.gte as any).getTime() / 1000)
        : undefined;

    const lte =
      query?.where?.messageTimestamp?.lte != null
        ? Math.floor(new Date(query.where!.messageTimestamp!.lte as any).getTime() / 1000)
        : undefined;

    const hasTsFilter = gte != null && lte != null;

    const tsFilter = hasTsFilter
      ? Prisma.sql`AND m."messageTimestamp" >= ${gte!} AND m."messageTimestamp" <= ${lte!}`
      : Prisma.sql``;

    const take = Number.isFinite(query?.take) ? Number(query!.take) : 20;
    const skip = Number.isFinite(query?.skip) ? Number(query!.skip) : 0;

    const limitSql = Prisma.sql`LIMIT ${take}`;
    const offsetSql = Prisma.sql`OFFSET ${skip}`;

    // --- página de dados ---
    const rows = await this.prismaRepository.$queryRaw<Array<{
      chatId: string | null;
      remoteJid: string;
      pushName: string | null;
      profilePicUrl: string | null;
      updatedAt: Date | null;
      isGroup: boolean;
      unreadMessages: number;

      lastMessageId: string | null;
      lastMessageKey: Prisma.JsonValue | null;
      lastMessagePushName: string | null;
      lastMessageParticipant: string | null;
      lastMessageMessageType: string | null;
      lastMessageMessage: Prisma.JsonValue | null;
      lastMessageContextInfo: Prisma.JsonValue | null;
      lastMessageSource: string | null;
      lastMessageMessageTimestamp: number | null;
      lastMessageInstanceId: string | null;
      lastMessageSessionId: string | null;
      lastMessageStatus: string | null;
    }>>`
      WITH last_msg AS (
        SELECT DISTINCT ON (m."key"->>'remoteJid')
          m."key"->>'remoteJid'                    AS "remoteJid",
          m."id"                                   AS "lastMessageId",
          m."key"                                  AS "lastMessageKey",
          CASE WHEN m."key"->>'fromMe' = 'true'
              THEN 'Você' ELSE m."pushName" END   AS "lastMessagePushName",
          m."participant"                          AS "lastMessageParticipant",
          m."messageType"                          AS "lastMessageMessageType",
          m."message"                              AS "lastMessageMessage",
          m."contextInfo"                          AS "lastMessageContextInfo",
          m."source"                               AS "lastMessageSource",
          m."messageTimestamp"                     AS "lastMessageMessageTimestamp",
          m."instanceId"                           AS "lastMessageInstanceId",
          m."sessionId"                            AS "lastMessageSessionId",
          m."status"                               AS "lastMessageStatus"
        FROM "Message" m
        WHERE m."instanceId" = ${this.instanceId}
          ${remoteJid ? Prisma.sql`AND m."key"->>'remoteJid' = ${remoteJid}` : Prisma.sql``}
          ${tsFilter}
        ORDER BY m."key"->>'remoteJid', m."messageTimestamp" DESC
      )
      SELECT
        ch."id"                                    AS "chatId",
        COALESCE(lm."remoteJid", ch."remoteJid")   AS "remoteJid",
        CASE
          WHEN COALESCE(lm."remoteJid", ch."remoteJid") LIKE '%@g.us'
            THEN ch."name"
          ELSE COALESCE(c."pushName", lm."lastMessagePushName", ch."name")
        END                                         AS "pushName",
        c."profilePicUrl"                           AS "profilePicUrl",
        COALESCE(
          CASE WHEN lm."lastMessageMessageTimestamp" IS NOT NULL
              THEN to_timestamp(lm."lastMessageMessageTimestamp"::double precision)
              ELSE NULL
          END,
          ch."updatedAt"
        )                                           AS "updatedAt",
        (COALESCE(lm."remoteJid", ch."remoteJid") LIKE '%@g.us') AS "isGroup",
        ch."unreadMessages"                         AS "unreadMessages",

        lm."lastMessageId",
        lm."lastMessageKey",
        lm."lastMessagePushName",
        lm."lastMessageParticipant",
        lm."lastMessageMessageType",
        lm."lastMessageMessage",
        lm."lastMessageContextInfo",
        lm."lastMessageSource",
        lm."lastMessageMessageTimestamp",
        lm."lastMessageInstanceId",
        lm."lastMessageSessionId",
        lm."lastMessageStatus"
      FROM "Chat" ch
      LEFT JOIN "Contact" c
        ON c."remoteJid" = ch."remoteJid"
      AND c."instanceId" = ch."instanceId"
      ${hasTsFilter
        ? Prisma.sql`INNER JOIN last_msg lm ON lm."remoteJid" = ch."remoteJid"`
        : Prisma.sql`LEFT JOIN  last_msg lm ON lm."remoteJid" = ch."remoteJid"`}
      WHERE ch."instanceId" = ${this.instanceId}
        ${remoteJid ? Prisma.sql`AND ch."remoteJid" = ${remoteJid}` : Prisma.sql``}
      ORDER BY lm."lastMessageMessageTimestamp" DESC NULLS LAST
      ${limitSql}
      ${offsetSql};
    `;

    // --- total (mesma seleção, sem LIMIT/OFFSET) ---
    const totalRows = await this.prismaRepository.$queryRaw<Array<{ total: number }>>`
      WITH last_msg AS (
        SELECT DISTINCT ON (m."key"->>'remoteJid')
          m."key"->>'remoteJid'    AS "remoteJid",
          m."messageTimestamp"     AS "lastMessageMessageTimestamp"
        FROM "Message" m
        WHERE m."instanceId" = ${this.instanceId}
          ${remoteJid ? Prisma.sql`AND m."key"->>'remoteJid' = ${remoteJid}` : Prisma.sql``}
          ${tsFilter}
        ORDER BY m."key"->>'remoteJid', m."messageTimestamp" DESC
      )
      SELECT COUNT(*)::int AS total
      FROM "Chat" ch
      LEFT JOIN "Contact" c
        ON c."remoteJid" = ch."remoteJid"
      AND c."instanceId" = ch."instanceId"
      ${hasTsFilter
        ? Prisma.sql`INNER JOIN last_msg lm ON lm."remoteJid" = ch."remoteJid"`
        : Prisma.sql`LEFT JOIN  last_msg lm ON lm."remoteJid" = ch."remoteJid"`}
      WHERE ch."instanceId" = ${this.instanceId}
        ${remoteJid ? Prisma.sql`AND ch."remoteJid" = ${remoteJid}` : Prisma.sql``};
    `;

    const total = totalRows?.[0]?.total ?? 0;

    // --- mapeamento DTO ---
    const items = (rows ?? []).map(r => {
      const lastMessage = r.lastMessageId
        ? {
            id: r.lastMessageId,
            key: r.lastMessageKey,
            pushName: r.lastMessagePushName ?? undefined,
            participant: r.lastMessageParticipant ?? undefined,
            messageType: r.lastMessageMessageType ?? undefined,
            message: r.lastMessageMessage ?? undefined,
            contextInfo: r.lastMessageContextInfo ?? undefined,
            source: r.lastMessageSource ?? undefined,
            messageTimestamp: r.lastMessageMessageTimestamp ?? undefined,
            instanceId: r.lastMessageInstanceId ?? undefined,
            sessionId: r.lastMessageSessionId ?? undefined,
            status: r.lastMessageStatus ?? undefined,
          }
        : undefined;

      return {
        remoteJid: r.remoteJid,
        pushName: r.pushName ?? undefined,
        isGroup: r.isGroup,
        id: r.chatId ?? null,
        profilePicUrl: r.profilePicUrl ?? undefined,
        updatedAt: r.updatedAt ?? undefined,
        lastMessage: lastMessage ? this.cleanMessageData(lastMessage) : undefined,
        unreadCount: r.unreadMessages ?? 0,
        isSaved: !!r.chatId,
      };
    });

    return {
      items,
      total,
      take,
      skip,
      hasMore: skip + take < total,
    };
  }


  public async fetchStatusMessage(query: any) {
    if (!query?.offset) {
      query.offset = 50;
    }

    if (!query?.page) {
      query.page = 1;
    }

    return await this.prismaRepository.messageUpdate.findMany({
      where: {
        instanceId: this.instanceId,
        remoteJid: query.where?.remoteJid,
        keyId: query.where?.id,
      },
      skip: query.offset * (query?.page === 1 ? 0 : (query?.page as number) - 1),
      take: query.offset,
    });
  }

  public async findChatByRemoteJid(remoteJid: string) {
    if (!remoteJid) return null;
    return await this.prismaRepository.chat.findFirst({
      where: {
        instanceId: this.instanceId,
        remoteJid: remoteJid,
      },
    });
  }

  public async fetchChats(query: any) {
    const remoteJid = query?.where?.remoteJid
      ? query?.where?.remoteJid.includes('@')
        ? query.where?.remoteJid
        : createJid(query.where?.remoteJid)
      : null;

    const where = {
      instanceId: this.instanceId,
    };

    if (remoteJid) {
      where['remoteJid'] = remoteJid;
    }

    const timestampFilter =
      query?.where?.messageTimestamp?.gte && query?.where?.messageTimestamp?.lte
        ? Prisma.sql`
        AND "Message"."messageTimestamp" >= ${Math.floor(new Date(query.where.messageTimestamp.gte).getTime() / 1000)}
        AND "Message"."messageTimestamp" <= ${Math.floor(new Date(query.where.messageTimestamp.lte).getTime() / 1000)}`
        : Prisma.sql``;

    const limit = query?.take ? Prisma.sql`LIMIT ${query.take}` : Prisma.sql``;
    const offset = query?.skip ? Prisma.sql`OFFSET ${query.skip}` : Prisma.sql``;

    const results = await this.prismaRepository.$queryRaw`
      WITH rankedMessages AS (
        SELECT DISTINCT ON ("Message"."key"->>'remoteJid') 
          "Contact"."id" as "contactId",
          "Message"."key"->>'remoteJid' as "remoteJid",
          CASE 
            WHEN "Message"."key"->>'remoteJid' LIKE '%@g.us' THEN COALESCE("Chat"."name", "Contact"."pushName")
            ELSE COALESCE("Contact"."pushName", "Message"."pushName")
          END as "pushName",
          "Contact"."profilePicUrl",
          COALESCE(
            to_timestamp("Message"."messageTimestamp"::double precision), 
            "Contact"."updatedAt"
          ) as "updatedAt",
          "Chat"."name" as "pushName",
          "Chat"."createdAt" as "windowStart",
          "Chat"."createdAt" + INTERVAL '24 hours' as "windowExpires",
          "Chat"."unreadMessages" as "unreadMessages",
          CASE WHEN "Chat"."createdAt" + INTERVAL '24 hours' > NOW() THEN true ELSE false END as "windowActive",
          "Message"."id" AS "lastMessageId",
          "Message"."key" AS "lastMessage_key",
          CASE
            WHEN "Message"."key"->>'fromMe' = 'true' THEN 'Você'
            ELSE "Message"."pushName"
          END AS "lastMessagePushName",
          "Message"."participant" AS "lastMessageParticipant",
          "Message"."messageType" AS "lastMessageMessageType",
          "Message"."message" AS "lastMessageMessage",
          "Message"."contextInfo" AS "lastMessageContextInfo",
          "Message"."source" AS "lastMessageSource",
          "Message"."messageTimestamp" AS "lastMessageMessageTimestamp",
          "Message"."instanceId" AS "lastMessageInstanceId",
          "Message"."sessionId" AS "lastMessageSessionId",
          "Message"."status" AS "lastMessageStatus"
        FROM "Message"
        LEFT JOIN "Contact" ON "Contact"."remoteJid" = "Message"."key"->>'remoteJid' AND "Contact"."instanceId" = "Message"."instanceId"
        LEFT JOIN "Chat" ON "Chat"."remoteJid" = "Message"."key"->>'remoteJid' AND "Chat"."instanceId" = "Message"."instanceId"
        WHERE "Message"."instanceId" = ${this.instanceId}
        ${remoteJid ? Prisma.sql`AND "Message"."key"->>'remoteJid' = ${remoteJid}` : Prisma.sql``}
        ${timestampFilter}
        ORDER BY "Message"."key"->>'remoteJid', "Message"."messageTimestamp" DESC
      )
      SELECT * FROM rankedMessages 
      ORDER BY "updatedAt" DESC NULLS LAST
      ${limit}
      ${offset};
    `;

    if (results && isArray(results) && results.length > 0) {
      const mappedResults = results.map((contact) => {
        const lastMessage = contact.lastMessageId
          ? {
              id: contact.lastMessageId,
              key: contact.lastMessage_key,
              pushName: contact.lastMessagePushName,
              participant: contact.lastMessageParticipant,
              messageType: contact.lastMessageMessageType,
              message: contact.lastMessageMessage,
              contextInfo: contact.lastMessageContextInfo,
              source: contact.lastMessageSource,
              messageTimestamp: contact.lastMessageMessageTimestamp,
              instanceId: contact.lastMessageInstanceId,
              sessionId: contact.lastMessageSessionId,
              status: contact.lastMessageStatus,
            }
          : undefined;

        return {
          id: contact.contactId || null,
          remoteJid: contact.remoteJid,
          pushName: contact.pushName,
          profilePicUrl: contact.profilePicUrl,
          updatedAt: contact.updatedAt,
          windowStart: contact.windowStart,
          windowExpires: contact.windowExpires,
          windowActive: contact.windowActive,
          lastMessage: lastMessage ? this.cleanMessageData(lastMessage) : undefined,
          unreadCount: contact.unreadMessages,
          isSaved: !!contact.contactId,
        };
      });

      return mappedResults;
    }

    return [];
  }

  public hasValidMediaContent(message: any): boolean {
    if (!message?.message) return false;

    const msg = message.message;

    // Se só tem messageContextInfo, não é mídia válida
    if (Object.keys(msg).length === 1 && Object.prototype.hasOwnProperty.call(msg, 'messageContextInfo')) {
      return false;
    }

    // Verifica se tem pelo menos um tipo de mídia válido
    const mediaTypes = [
      'imageMessage',
      'videoMessage',
      'stickerMessage',
      'documentMessage',
      'documentWithCaptionMessage',
      'ptvMessage',
      'audioMessage',
    ];

    return mediaTypes.some((type) => msg[type] && Object.keys(msg[type]).length > 0);
  }
}
