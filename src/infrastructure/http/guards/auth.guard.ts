import { CanActivate, ExecutionContext, Inject, Injectable, Optional } from '@nestjs/common';
import type { Request } from 'express';

export interface ProviderIdentityPort {
  validateProvider(providerId: string, token?: string): Promise<boolean>;
}

@Injectable()
export class NoopProviderIdentityPort implements ProviderIdentityPort {
  async validateProvider(_providerId: string, _token?: string): Promise<boolean> {
    // Ponto de extensão no-op: em produção integra com Keycloak / Zitadel (OIDC/OAuth2)
    return true;
  }
}

@Injectable()
export class AuthGuard implements CanActivate {
  constructor(
    @Optional()
    @Inject('ProviderIdentityPort')
    private readonly identityPort?: ProviderIdentityPort,
  ) {}

  async canActivate(context: ExecutionContext): Promise<boolean> {
    const request = context.switchToHttp().getRequest<Request>();
    const path = request.path || request.url || '';

    // Endpoints de health são explicitamente abertos
    if (path.startsWith('/health')) {
      return true;
    }

    if (this.identityPort) {
      const providerId = (request.headers['x-provider-id'] as string) || 'default-provider';
      const token = request.headers.authorization?.replace(/^Bearer\s+/, '');
      return this.identityPort.validateProvider(providerId, token);
    }

    return true;
  }
}
